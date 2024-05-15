"""WDQS data reload"""

import argparse
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from os import getpid
from time import sleep
from typing import Optional

from spicerack import RemoteHosts, Reason, Remote, Netbox, Kafka, ConftoolEntity, AlertingHosts, PuppetHosts
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.kafka import ConsumerDefinition
from transferpy.Transferer import Transferer
from wmflib.prometheus import Prometheus

from cookbooks.sre.wdqs import check_hosts_are_valid, wait_for_updater, get_site, MUTATION_TOPICS
from cookbooks.sre.wdqs import get_hostname, StopWatch, is_behind_lvs

logger = logging.getLogger(__name__)

DAYS_IT_TAKES_TO_RELOAD = 17
DAYS_KAFKA_RETAINED = 30

# TODO: hardcoding this is far from ideal, there might be ways to infer this from the puppetdb and possibly select
#  one of the hosts with "A:stat and P{C:hdfs_tools}" and run "df -B1 --output=avail $some_path | tail -n1" to infer
#  what hosts have enough space. We could also consider dropping this default completely and ask the operator to always
#  select it for us.
DEFAULT_STAT_HOST = "stat1011.eqiad.wmnet"
DEFAULT_STAT_TMP_FOLDER = "/srv/analytics-search/wdqs_reload_temp_folder"
DEFAULT_ANALYTICS_KERB_USER = "analytics-search"


class DumpsSource(Enum):
    """Enum with types of source we support.

    NFS will munge, HDFS assumes that the dumps are pre-munged.
    """

    NFS = 1
    HDFS = 2


@dataclass
class ReloadProfile:
    """Data class holding the bits required for a reload."""

    dumps_source: DumpsSource
    source_folders: list[str]
    chunk_format: str
    mutation_topic: str
    updater_service: str
    blazegraph_service: str
    data_loaded_flag: str
    journal_path: str
    namespace: str


WDQS_OPTIONS = {
    'data_loaded_flag': '/srv/wdqs/data_loaded',
    'updater_service': 'wdqs-updater',
    'blazegraph_service': 'wdqs-blazegraph',
    'namespace': 'wdq',
    'journal_path': '/srv/wdqs/wikidata.jnl'
}

WCQS_OPTIONS = {
    'data_loaded_flag': '/srv/query_service/data_loaded',
    'updater_service': 'wcqs-updater',
    'blazegraph_service': 'wcqs-blazegraph',
    'namespace': 'wcq',
    'journal_path': '/srv/query_service/wcqs.jnl'
}

RELOAD_PROFILES = {
    'wikidata_full': ReloadProfile(
        dumps_source=DumpsSource.HDFS,
        source_folders=['/srv/wdqs/dumps_from_hdfs'],
        chunk_format='wikidata_full.%04d.nt.gz',
        mutation_topic=MUTATION_TOPICS['wikidata_full'],
        **WDQS_OPTIONS
    ),
    'wikidata_main': ReloadProfile(
        dumps_source=DumpsSource.HDFS,
        source_folders=['/srv/wdqs/dumps_from_hdfs'],
        chunk_format='wikidata_main.%04d.nt.gz',
        mutation_topic=MUTATION_TOPICS['wikidata_main'],
        **WDQS_OPTIONS
    ),
    'scholarly_articles': ReloadProfile(
        dumps_source=DumpsSource.HDFS,
        source_folders=['/srv/wdqs/dumps_from_hdfs'],
        chunk_format='scholarly_articles.%04d.nt.gz',
        mutation_topic=MUTATION_TOPICS['scholarly_articles'],
        **WDQS_OPTIONS
    ),
    'wikidata': ReloadProfile(
        dumps_source=DumpsSource.NFS,
        source_folders=['/srv/wdqs/munged', '/srv/wdqs/lex-munged'],
        chunk_format='wikidump-%09d.ttl.gz',
        mutation_topic=MUTATION_TOPICS['wikidata'],
        **WDQS_OPTIONS
    ),
    'commons': ReloadProfile(
        dumps_source=DumpsSource.NFS,
        source_folders=['/srv/query_service/munged'],
        chunk_format='wikidump-%09d.ttl.gz',
        mutation_topic=MUTATION_TOPICS['commons'],
        **WCQS_OPTIONS
    )
}


class Runnable:
    """Runnable class, defaults to doing nothing"""

    def run(self) -> None:
        """Do nothing"""

    @property
    def runtime_description(self) -> str:
        """Runtime description."""
        return ""


class DataReload(CookbookBase):
    """The DataReload cookbook.

    Usage example:
        # for lvs-managed hosts
        cookbook sre.wdqs.data-reload --reload-data wikidata --reason "bring new hosts into rotation" \
        --task-id T301167 wdqs1004.eqiad.wmnet

        # hosts not managed by lvs (note the --no-depool flag)
        cookbook sre.wdqs.data-reload --no-depool --reload-data wikidata \
        --reason "reloading on test host" --task-id T301167 wdqs1009.eqiad.wmnet
    """

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('host', help='select a single WDQS host.')
        parser.add_argument('--task-id', help='task id for the change')
        parser.add_argument('--reason', required=True, help='Administrative Reason')
        parser.add_argument('--downtime', type=int, default=336, help='Hour(s) of downtime')
        parser.add_argument('--no-depool', action='store_true',
                            help='Don\'t depool host (use for non-lvs-managed hosts)')
        parser.add_argument('--reload-data', required=True, choices=RELOAD_PROFILES.keys(),
                            help='Type of data to reload')
        parser.add_argument('--from-hdfs', help='full path in hdfs from where the dumps should be taken')
        parser.add_argument('--stat-host', default=DEFAULT_STAT_HOST,
                            help='stat host to use as intermediary to extract data out of HDFS')
        parser.add_argument("--kerberos-user", default=DEFAULT_ANALYTICS_KERB_USER,
                            help='Kerberos user to use when connecting to HDFS')
        parser.add_argument("--stat-local-folder", default=DEFAULT_STAT_TMP_FOLDER)
        parser.add_argument("--position-kafka-offsets", default=True,
                            help='Do not attempt to position kafka offsets, '
                                 'only useful for hosts that have the updater disabled.')
        return parser

    @staticmethod
    def _query_single_host(remote: Remote, host: str) -> RemoteHosts:
        remote_host = remote.query(host)
        if len(remote_host) != 1:
            raise ValueError("Only one host is needed. Not {total}({source})".
                             format(total=len(remote_host), source=remote_host))
        return remote_host

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Prepare the cookbook."""
        remote = self.spicerack.remote()
        remote_host = DataReload._query_single_host(remote, args.host)
        check_hosts_are_valid(remote_host, remote)
        reload_profile: ReloadProfile = RELOAD_PROFILES[args.reload_data]

        prep_command: Runnable
        if reload_profile.dumps_source == DumpsSource.NFS:
            prep_command = MungeFromNFS(remote_host, args.reload_data)
        elif reload_profile.dumps_source == DumpsSource.HDFS:
            if len(reload_profile.source_folders) > 1:
                raise ValueError("Only one data_folder expected when loading from HDFS")

            if args.from_hdfs is None:
                raise ValueError("--from-hdfs must be specified when using a profile "
                                 "that sources it data from HDFS")
            prep_command = HdfsCopy(
                stat_host=DataReload._query_single_host(remote, args.stat_host),
                kerberos_user=args.kerberos_user,
                hdfs_path=args.from_hdfs,
                hdfs_local_path=args.stat_local_folder,
                query_service_host=remote_host,
                query_service_data_path=reload_profile.source_folders[0])
        else:
            raise ValueError("Unsupported type of source")

        postload: Runnable
        if args.position_kafka_offsets:
            postload = UpdaterRestart(
                netbox=self.spicerack.netbox(),
                kafka=self.spicerack.kafka(),
                query_service_host=remote_host,
                prometheus=self.spicerack.prometheus(),
                mutation_topic=reload_profile.mutation_topic,
                updater_service=reload_profile.updater_service)
        else:
            # noop
            postload = Runnable()

        return DataReloadRunner(
            puppet=self.spicerack.puppet(remote_host),
            confctl=self.spicerack.confctl('node'),
            alerting_host=self.spicerack.alerting_hosts(remote_host.hosts),
            query_service_host=remote_host,
            preparation_step=prep_command,
            reload_profile=reload_profile,
            profile_name=args.reload_data,
            no_depool=args.no_depool,
            reason=self.spicerack.admin_reason(args.reason, task_id=args.task_id),
            downtime=args.downtime,
            postload_step=postload)


class DataReloadRunner(CookbookRunnerBase):
    """The data reload cookbook runner used by the cookbook."""

    def __init__(self,  # pylint: disable=too-many-arguments
                 alerting_host: AlertingHosts,
                 confctl: ConftoolEntity,
                 puppet: PuppetHosts,
                 query_service_host: RemoteHosts,
                 preparation_step: Runnable,
                 postload_step: Runnable,
                 reload_profile: ReloadProfile,
                 profile_name: str,
                 no_depool: bool,
                 reason: Reason,
                 downtime: int):
        """Create the runner"""
        self.alerting_host = alerting_host
        self.confctl = confctl
        self.puppet = puppet
        self.query_service_host = query_service_host
        self.preparation_step = preparation_step
        self.postload_step = postload_step
        self.reload_profile = reload_profile
        self.profile_name = profile_name
        self.no_depool = no_depool
        self.reason = reason
        self.downtime = downtime

    @property
    def runtime_description(self) -> str:
        """The runtime description"""
        return (f"reloading {self.profile_name} on {self.query_service_host.hosts} "
                f"from {self.reload_profile.dumps_source} ({self.preparation_step.runtime_description})")

    @property
    def lock_args(self) -> LockArgs:
        """Allow only one reload per host at a time."""
        return LockArgs(suffix=str(self.query_service_host.hosts), concurrency=1,
                        ttl=int(timedelta(hours=self.downtime * 2).total_seconds()))

    def run(self) -> None:
        """The run method"""
        self.preparation_step.run()
        with self.alerting_host.downtimed(self.reason, duration=timedelta(hours=self.downtime)):
            if self.no_depool or not is_behind_lvs(self.confctl, self.query_service_host):
                self._reload_wikibase()
            else:
                with self.confctl.change_and_revert('pooled', True, False, name=self.query_service_host.hosts[0]):
                    sleep(180)
                    self._reload_wikibase()

    def _load_data_command(self, dump_path: str) -> str:
        """Build the loadData command to use for importing a folder of RDF files."""
        return (f"bash /srv/deployment/wdqs/wdqs/loadData.sh -n {self.reload_profile.namespace} "
                f"-d {dump_path} "
                f"-f '{self.reload_profile.chunk_format}'")

    def _reload_wikibase(self) -> None:
        """Execute commands on host to reload wikidata/commons data."""
        logger.info('Prepare to load wikidata data for blazegraph')
        with self.puppet.disabled(self.reason):
            # TODO: consider keeping the journal around if space allows
            #  and use CookbookRunnerBase.rollback to restore the system
            self.query_service_host.run_sync(
                f'rm -fv {self.reload_profile.data_loaded_flag}',
                f'systemctl stop {self.reload_profile.updater_service}',
                f'systemctl stop {self.reload_profile.blazegraph_service}',
                f'rm -fv {self.reload_profile.journal_path}',
                f'systemctl start {self.reload_profile.blazegraph_service}',
            )
        # wait for blazegraph to start
        # TODO: sleeping is far from ideal, consider using another technique (ping some blazegraph API?)
        #  to wait for its availability
        sleep(60)
        logger.info('Loading dump')
        watch = StopWatch()
        self.query_service_host.run_sync(
            f'test -f {self.reload_profile.journal_path}',
            *[self._load_data_command(path) for path in self.reload_profile.source_folders]
        )

        self.query_service_host.run_sync(f'touch {self.reload_profile.data_loaded_flag}')

        logger.info('Loaded dumps in %s', watch.elapsed())
        self.postload_step.run()


class UpdaterRestart(Runnable):
    """Position kafka offsets, restart the updater and wait"""

    def __init__(self,  # pylint: disable=too-many-arguments
                 netbox: Netbox,
                 kafka: Kafka,
                 query_service_host: RemoteHosts,
                 prometheus: Prometheus,
                 mutation_topic: str,
                 updater_service: str):
        """Create the runner"""
        self.netbox = netbox
        self.kafka = kafka
        self.query_service_host = query_service_host
        self.prometheus = prometheus
        self.mutation_topic = mutation_topic
        self.updater_service = updater_service

    def run(self) -> None:
        """Position kafka offsets, restart the updater and wait"""
        hostname = get_hostname(self.query_service_host.hosts[0])
        site = get_site(hostname, self.netbox)
        consumer_definition = ConsumerDefinition(site, 'main',
                                                 get_hostname(self.query_service_host.hosts[0]))
        timestamp = self._extract_kafka_timestamp_from_sparql()
        validate_dump_age(timestamp, 'after_reload')
        topic_offsets = {self.mutation_topic: int(timestamp.timestamp() * 1000)}
        self.kafka.set_consumer_position_by_timestamp(consumer_definition, topic_offsets)
        self.query_service_host.run_sync(f'systemctl start {self.updater_service}')
        logger.info('Data reload for blazegraph is complete. Waiting for updater to catch up '
                    'on %s@%s', hostname, site)
        watch = StopWatch()
        wait_for_updater(self.prometheus, site, self.query_service_host)
        logger.info('Caught up on updates in %s', watch.elapsed())

    def _extract_kafka_timestamp_from_sparql(self) -> datetime:
        """Run a SPARQL query to extract the oldest dump timestamp."""
        cmd = ("set -o pipefail; "
               "echo 'SELECT ?dumpdate {"
               " wikibase:Dump schema:dateModified ?dumpdate "
               "} ORDER BY ASC(?dumpdate) LIMIT 1' | "
               "curl -f -s --data-urlencode query@- http://localhost/sparql?format=json | "
               "jq -r .results.bindings[0].dumpdate.value")
        (_, msg_output) = next(self.query_service_host.run_sync(cmd))
        timestamp = msg_output.message()
        logger.info('[extract_kafka_timestamp_from_sparql] found %s', timestamp)
        return parse_iso_dt(timestamp)


class HdfsCopy(Runnable):
    """Class doing a copy from HDFS to a query service host using a stat host a intermediary"""

    def __init__(self,  # pylint: disable=too-many-arguments
                 stat_host: RemoteHosts,
                 kerberos_user: str,
                 hdfs_path: str,
                 hdfs_local_path: str,
                 query_service_host: RemoteHosts,
                 query_service_data_path: str):
        """Create the runner"""
        self.stat_host = stat_host
        self.kerberos_user = kerberos_user
        self.hdfs_path = hdfs_path
        self.hdfs_local_path = hdfs_local_path
        self.query_service_host = query_service_host
        self.query_service_data_path = query_service_data_path

    def _check_free_space(self,
                          path: str,
                          additional_size: int,
                          threshold: float) -> None:
        """Check that enough free space is available

        check the device hosting path to store "additional_size" bytes more and still have "threshold"
        of free space.
        @param path: the local path on the stat machine
        @param additional_size: the additional_size
        @param threshold: the min threshold of remaining free space on the device
        @raise RuntimeError if threshold is not met
        """
        cmd = f"set -o pipefail; df --output=size,used -B1 '{path}' | tail -1"
        (_, msg_output) = next(self.stat_host.run_sync(cmd))
        (size, used) = [int(s) for s in msg_output.message().strip().split(" ", 2)]
        if ((size - (additional_size + used)) / size) < threshold:
            raise RuntimeError("Not enough space left on device to continue.")

    def _get_dump_size_from_hdfs(self) -> int:
        """Calculate the size of the dumps stored in HDFS.

        @return: the total size of the dump
        """
        cmd = (f'sudo -u {self.kerberos_user} kerberos-run-command {self.kerberos_user} '
               f'hdfs dfs -du -s "{self.hdfs_path}"')
        (_, msg_output) = next(self.stat_host.run_sync(cmd))
        lines = msg_output.lines()
        return int(re.sub(r"^(\d+)\s+.*$", next(lines), r"\1"))

    def _extract_from_hdfs(self, local_path: str) -> None:
        """Download the dump from HDFS and store it locally.

        @param local_path: the local path to download to
        """
        size = self._get_dump_size_from_hdfs()
        self._check_free_space(local_path, size, .25)
        self.stat_host.run_sync(f'sudo -u {self.kerberos_user} kerberos-run-command {self.kerberos_user} '
                                f'hdfs-rsync --delete --exclude "_*" "{self.hdfs_path}" "{local_path}"')

    def _cleanup_hdfs_temp_path(self, folder: str) -> None:
        """Cleanup gz files in stat_host:folder and remove the folder.

        @param folder: the folder to cleanup
        """
        self.stat_host.run_sync(f"find {folder} -maxdepth 1 -type f -name '*.gz' | xargs rm",
                                f"rmdir {folder}")

    def _prepare_hdfs_local_path(self) -> None:
        """Prepare the local path that will hold the temp path reveiving the content from HDFS"""
        self.stat_host.run_sync(f"mkdir -p {self.hdfs_local_path}",
                                f"chown {self.kerberos_user} {self.hdfs_local_path}")

    def _transfer_dump(self, source_folder: str) -> None:
        """Transfer dump files from source_host:source_folder to dest_host:target_folder.

        @param source_folder: the source folder on the source host
        @return:
        """
        transfer = Transferer(self.stat_host, source_folder,
                              [self.query_service_host], self.query_service_data_path)
        transfer.run()

    @property
    def runtime_description(self) -> str:
        """Runtime description."""
        return f"{self.hdfs_path} using {self.stat_host}"

    def run(self) -> None:
        """Transfer the dump from HDFS to the query service node."""
        logger.info("Creating %s:%s and setting %s as owner",
                    self.stat_host, self.hdfs_local_path, self.kerberos_user)
        self._prepare_hdfs_local_path()

        tmpdir = f"{self.hdfs_local_path}/reload.{getpid()}.{int(datetime.now().timestamp())}"

        logger.info("Extracting dumps from hdfs %s to %s:%s",
                    self.hdfs_path, self.stat_host, tmpdir)
        self._extract_from_hdfs(tmpdir)

        logger.info("Copying dumps from %s:%s to "
                    "%s:%s",
                    self.stat_host, tmpdir, self.query_service_host, self.query_service_data_path)
        self._transfer_dump(tmpdir)

        logger.info("Cleaning up %s:%s",
                    self.stat_host, tmpdir)
        self._cleanup_hdfs_temp_path(tmpdir)


@dataclass
class NfsDump:
    """Information about a dump in NFS. To drop once we solely rely on HDFS."""

    read_path: str
    munge_path: str
    munge_jar_args: Optional[str] = None


class MungeFromNFS(Runnable):
    """Munge operation"""

    NFS_DUMPS = {
        'wikidata': NfsDump(
            read_path='/mnt/nfs/dumps-clouddumps1001.wikimedia.org/wikidatawiki/entities/latest-all.ttl.bz2',
            munge_path='/srv/wdqs/munged',
        ),
        'lexeme': NfsDump(
            read_path='/mnt/nfs/dumps-clouddumps1001.wikimedia.org/wikidatawiki/entities/latest-lexemes.ttl.bz2',
            munge_path='/srv/wdqs/lex-munged',
        ),
        'commons': NfsDump(
            read_path='/mnt/nfs/dumps-clouddumps1001.wikimedia.org/commonswiki/entities',
            munge_path='/srv/query_service/munged',
            munge_jar_args=' --wikibaseHost commons.wikimedia.org'
                           ' --conceptUri http://www.wikidata.org'
                           ' --commonsUri https://commons.wikimedia.org'
        )
    }
    query_service_host: RemoteHosts
    dumps: list[NfsDump]

    def __init__(self, query_service_host: RemoteHosts, profile: str):
        """Builds the munger preparation"""
        self.query_service_host: RemoteHosts = query_service_host
        if profile == "wikidata":
            self.dumps = [self.NFS_DUMPS["wikidata"], self.NFS_DUMPS["lexeme"]]
        elif profile == "commons":
            self.dumps = [self.NFS_DUMPS["commons"]]
        else:
            raise ValueError(f"Unsupported profile {profile}")

    def _extract_kafka_timestamp(self) -> datetime:
        """Given a remote_host and journal type, parse and return the correct kafka timestamp."""
        dump_path = self.dumps[0].read_path
        cmd = "bzcat {} | head -50 | grep '^wikibase:Dump' -A 5 | grep 'schema:dateModified'".format(dump_path)
        status = next(self.query_service_host.run_sync(cmd))
        timestamp = str(list(status[1].lines())).split('"')[1]
        logger.info('[extract_kafka_timestamp] found %s', timestamp)
        return parse_iso_dt(timestamp)

    def _munge(self) -> None:
        """Run munger for main database and lexeme"""
        logger.info('Running munger for main database and then lexeme')
        stop_watch = StopWatch()
        for dump in self.dumps:
            logger.info('munging %s', dump.munge_path)
            stop_watch.reset()
            self.query_service_host.run_sync(
                "rm -rf {munge_path} && mkdir -p {munge_path} && bzcat {path} | "
                "/srv/deployment/wdqs/wdqs/munge.sh -f - -d {munge_path} -- --skolemize {munge_jar_args}"
                .format(path=dump.read_path,
                        munge_path=dump.munge_path,
                        munge_jar_args=str(dump.munge_jar_args or '')))
            logger.info('munging %s completed in %s', dump.munge_path, stop_watch.elapsed())

    @property
    def runtime_description(self) -> str:
        """Runtime description."""
        return f'munging data to {", ".join([d.munge_path for d in self.dumps])}'

    def run(self) -> None:
        """Munge dumps located in NFS."""
        # Get and validate kafka timestamp
        kafka_timestamp = self._extract_kafka_timestamp()
        validate_dump_age(kafka_timestamp, check_time="before_reload")
        self._munge()


def validate_dump_age(dump_date: datetime, check_time: str = "before_reload") -> None:
    """Given a timestamp, confirm that it fits requirements. Err/exit if not."""
    right_now_date = datetime.now()
    current_age = (right_now_date - dump_date).days
    if check_time == "before_reload":
        max_age = DAYS_KAFKA_RETAINED - DAYS_IT_TAKES_TO_RELOAD
        if current_age > max_age:
            raise RuntimeError(f"Dump age must be {max_age} days or less. Detected age: {current_age} days")
    elif check_time == "after_reload":
        if current_age > DAYS_KAFKA_RETAINED:
            raise RuntimeError(f"Current data is {current_age} days old, exceeding Kafka retention time of "
                               f"{DAYS_KAFKA_RETAINED} days")
    # if we made it this far, something is wrong.
    else:
        raise RuntimeError(f"Unknown error, check values passed to {validate_dump_age}")


def parse_iso_dt(timestamp: str) -> datetime:
    """Parse a datetime in iso8601 format.

    @param timestamp: the string to parse
    @return: the datetime representation
    @raise ValueError if the parsed datetime is not UTC
    """
    # Workaround python limitations not supporting trailing Z
    # TODO: remove once running python > 3.11
    timestamp = re.sub(r"(?<=\d)Z$", "+00:00", timestamp)
    dt = datetime.fromisoformat(timestamp)
    if dt.tzinfo != timezone.utc:
        raise ValueError(f'Parsed a suspicious datetime "{timestamp}" that is not UTC')
    return dt
