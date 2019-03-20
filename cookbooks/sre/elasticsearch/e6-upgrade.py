"""Rolling upgrade of elasticsearch service (elasticsearch 5.6.14 to 6.5.4 migration)

Order of operations for the upgrade:
 - upload elasticsearch-oss-6.5.4 and new plugins to our apt repo
 - make sure the elastic65 component is configured on the target nodes (via puppet)
 - make sure apt-get has been updated
 - disable puppet on target nodes (nodes of the cluster we plan to restart)
 - push & merge a puppet patch to switch target cluster to elasticsearch 6
 - run this cookbook
   - stop elasticsearch 5.6.14
   - install elasticsearch 6.5.4
   - run puppet to activate the new elastic6 setup
"""
import logging

from cumin.transports import Command
from spicerack.remote import RemoteExecutionError

from cookbooks.sre.elasticsearch import argument_parser_base, post_process_args, execute_on_clusters

__title__ = 'Rolling upgrade of elasticsearch service (elasticsearch 5.6.14 to 6.5.4 migration)'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __doc__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    icinga = spicerack.icinga()
    elasticsearch_clusters = spicerack.elasticsearch_clusters(args.clustergroup)
    reason = spicerack.admin_reason(args.admin_reason, task_id=args.task_id)

    def upgrade_elasticsearch(nodes):
        puppet = spicerack.puppet(nodes._remote_hosts)  # pylint: disable=protected-access

        puppet.check_disabled()

        nodes.stop_elasticsearch()

        packages = ['elasticsearch-oss', 'elasticsearch-', 'wmf-elasticsearch-search-plugins=6.5.4-1~stretch']

        nodes.get_remote_hosts().run_async(
            # save the previous instance list (which will be changed by puppet)
            'cp -v /etc/elasticsearch/instances /tmp/previous-elasticsearch-instances'
        )

        install_results = nodes.get_remote_hosts().run_async(
            # TODO: implement a generic and robust package upgrade mechanism in spicerack
            # upgrade the packages before switching the config es6
            # package names have changed use a hack to remove&install in one apt command
            # letting puppet run would cause it to fail since it'll install elasticsearch-oss without
            # removing elasticsearch (which would fail)
            Command(
                'DEBIAN_FRONTEND=noninteractive apt-get {options} install {packages}'.format(
                    options='-y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold"',
                    packages=' '.join(packages)),
                ok_codes=[])
        )

        # log output of apt-get, just in case
        for nodeset, output in install_results:
            logger.warning('Output for %s', nodeset)
            logger.warning(output.message().decode())

        try:
            # elasticsearch-oss isn't installed, something went wrong with apt-get install
            nodes.get_remote_hosts().run_async('dpkg -l elasticsearch-oss')
        except RemoteExecutionError:
            raise RemoteExecutionError(1, "elasticsearch-oss wasn't installed properly")

        nodes.get_remote_hosts().run_async(
            # reinstall the plugins because for some reasons the previous command leaves the plugin dir empty
            'apt-get install --reinstall wmf-elasticsearch-search-plugins'
        )

        # run puppet
        puppet.run(enable_reason=reason)

        # stop and cleanup old units
        nodes.get_remote_hosts().run_async(
            'rm -v /lib/systemd/system/elasticsearch_5@.service',
            'systemctl daemon-reload',
            'cat /tmp/previous-elasticsearch-instances | xargs systemctl reset-failed || true',
            'rm -v /tmp/previous-elasticsearch-instances',
        )

    execute_on_clusters(
        elasticsearch_clusters, icinga, reason, spicerack, args.nodes_per_run,
        args.clustergroup, args.start_datetime, args.with_lvs, args.wait_for_green, upgrade_elasticsearch
    )
