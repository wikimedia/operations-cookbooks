"""Rotate the CephX keys of Ceph OSD daemons to the aes256k key type."""

import json
import logging
from argparse import ArgumentParser
from datetime import timedelta

from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError, RemoteHosts

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase

logger = logging.getLogger(__name__)

CLUSTER_CHOICES = ("cephosd-codfw", "cephosd-eqiad")

KEY_TYPE = "aes256k"
# The BlueStore label field that holds the OSD's CephX key.
LABEL_KEY = "osd_key"
# Root-only scratch directory for the rotated keyring, removed on success.
SCRATCH_DIR = "/root/.osd-key-rotation"
# The rotation deliberately leaves the cluster in HEALTH_ERR until every key has been
# replaced, so a plain HEALTH_OK gate would never pass while this cookbook has work to
# do. Accept the expected insecure-key messages and nothing else, so that a degraded
# placement group or an unrelated problem still stops us.
HEALTH_CHECK = (
    "ceph health detail --format json "
    "| jq -e '[.checks | keys[] | select(startswith(\"AUTH_INSECURE\") | not)] | length == 0' > /dev/null"
)
# Gate between OSDs, inside the noout window. HEALTH_CHECK cannot be reused there,
# because noout raises OSD_FLAGS for the whole host. This asks the narrower question
# that actually matters: has the data recovered from the OSD we just restarted. Benign
# suffixes such as active+clean+scrubbing+deep are accepted, because a scrub is almost
# always running on a cluster this size and does not mean the PG is at risk.
PGS_CLEAN = (
    "ceph pg stat --format json "
    "| jq -e '[.pg_summary.num_pg_by_state[] | select(.name | startswith(\"active+clean\") | not)] | length == 0'"
    " > /dev/null"
)


class RotateOsdKeys(SREBatchBase):
    """Rotate the CephX key of every OSD on a Ceph server to the aes256k key type

    Ceph 19.2.6 adds the aes256k CephX key type for CVE-2025-30156, and a cluster
    whose keys still use the older aes type reports HEALTH_ERR. OSD keys are owned by
    the cluster rather than by Puppet, so they are rotated with `ceph auth rotate`.

    An OSD keeps its key in three places: the monitors' auth database, the keyring in
    its data directory, and the `osd_key` field of its BlueStore label. All three have
    to be updated together, and the daemon has to be stopped to write the label.

    One host is processed at a time, with `noout` set for that host. Within a host each
    OSD is stopped, rotated and started again on its own, waiting for every placement
    group to return to active+clean before moving on to the next.

    Pass --per-host to stop all of a host's OSDs together instead. That is faster, but
    our failure domain is the host, so it leaves every placement group in the cluster
    degraded until the whole host is back rather than only those on one OSD. No data is
    at risk with three hosts and three replicas, but the degraded window lasts minutes
    rather than the seconds a plain restart takes.

    Puppet is disabled for the duration, because the activate exec in ceph::osd is
    guarded on `systemctl is-active` and would otherwise restart an OSD that this
    cookbook has deliberately stopped.

    OSDs that the cluster does not report as using an insecure key type are skipped, so
    an interrupted run can simply be repeated. Pass --force to rotate regardless.

    Examples:
        # Rotate every OSD key in the codfw cluster, one OSD at a time
        cookbook sre.ceph.rotate-osd-keys \
            --alias cephosd-codfw \
            --reason "CVE-2025-30156 key rotation" \
            --task-id T437447

        # Rehearse on a single OSD before committing to the rest
        cookbook sre.ceph.rotate-osd-keys \
            --query 'P{cephosd2001.codfw.wmnet}' \
            --osd 0 \
            --reason "CVE-2025-30156 key rotation" \
            --task-id T437447

        # Take a whole host's OSDs down at once
        cookbook sre.ceph.rotate-osd-keys \
            --query 'P{cephosd2001.codfw.wmnet}' \
            --per-host \
            --reason "CVE-2025-30156 key rotation" \
            --task-id T437447
    """

    valid_actions = ("rotate_keys",)
    batch_max = 1  # One host at a time, whatever the granularity within it
    grace_sleep = 60
    min_grace_sleep = 1

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument(
            "--osd",
            nargs="+",
            default=None,
            help="Only rotate these OSD ids, for example --osd 0 1. Defaults to all "
            "OSDs found on each host.",
        )
        parser.add_argument(
            "--per-host",
            action="store_true",
            help="Stop all of a host's OSDs together rather than one at a time. "
            "Faster, but leaves every placement group in the cluster degraded for "
            "the duration.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Rotate even an OSD that the cluster does not report as using an "
            "insecure key type.",
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RotateOsdKeysRunner(args, self.spicerack)


class RotateOsdKeysRunner(SREBatchRunnerBase):
    """Ceph OSD key rotation runner"""

    @property
    def allowed_aliases(self) -> list[str]:
        """Required by SREBatchRunnerBase"""
        return list(CLUSTER_CHOICES)

    @property
    def restart_daemons(self) -> list[str]:
        """Required by SREBatchRunnerBase, unused by this cookbook"""
        return []

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Stop the cluster rebalancing while this host's OSDs come and go"""
        self._wait_for(hosts, HEALTH_CHECK)
        # $(hostname) is evaluated on the target host, giving eg cephosd2001
        self._wait_for(hosts, "ceph osd set-group noout $(hostname)")
        super().pre_action(hosts)

    def post_action(self, hosts: RemoteHosts) -> None:
        """Re-enable rebalancing and confirm the cluster is healthy again"""
        # noout is cleared first, so OSD_FLAGS has gone before the health check runs
        self._wait_for(hosts, "ceph osd unset-group noout $(hostname)")
        self._wait_for(hosts, HEALTH_CHECK)
        super().post_action(hosts)

    def _rotate_keys_action(self, hosts: RemoteHosts, reason) -> None:
        """Rotate the CephX key of each OSD on this host"""
        devices = self._osd_devices(hosts)
        if not devices:
            raise RuntimeError(f"Found no OSDs on {hosts}")

        wanted = self._selected_osds(hosts, devices)
        if not wanted:
            logger.info("%s: no OSD needs its key rotating, nothing to do", hosts)
            return

        logger.info("%s: rotating %d OSD keys: %s", hosts, len(wanted), " ".join(wanted))
        puppet = self._spicerack.puppet(hosts)
        with puppet.disabled(reason):
            if self._args.per_host:
                self._rotate_together(hosts, wanted, devices)
            else:
                for osd_id in wanted:
                    self._rotate_one(hosts, osd_id, devices[osd_id])

    def _rotate_together(self, hosts: RemoteHosts, osd_ids: list[str], devices: dict[str, str]) -> None:
        """Stop every selected OSD, rotate them all, then start them again"""
        # ok-to-stop takes OSD ids, so ask the CRUSH map which ones this host holds
        self._wait_for(hosts, "ceph osd ok-to-stop $(ceph osd ls-tree $(hostname) | tr '\\n' ' ')")
        hosts.run_sync("systemctl stop ceph-osd.target")
        for osd_id in osd_ids:
            self._update_key(hosts, osd_id, devices[osd_id])
        hosts.run_sync("systemctl start ceph-osd.target")
        self._wait_for(hosts, PGS_CLEAN)

    def _rotate_one(self, hosts: RemoteHosts, osd_id: str, device: str) -> None:
        """Stop one OSD, rotate its key, start it and wait for the cluster to settle"""
        logger.info("%s: rotating osd.%s on %s", hosts, osd_id, device)
        self._wait_for(hosts, f"ceph osd ok-to-stop {osd_id}")
        hosts.run_sync(f"systemctl stop ceph-osd@{osd_id}")
        self._update_key(hosts, osd_id, device)
        hosts.run_sync(f"systemctl start ceph-osd@{osd_id}")
        # Wait for the data to recover before touching the next OSD. ok-to-stop is no
        # use here: immediately after a start the OSD holds no PGs yet, so it returns
        # true for the trivial reason that stopping it would affect nothing.
        self._wait_for(hosts, PGS_CLEAN)

    def _update_key(self, hosts: RemoteHosts, osd_id: str, device: str) -> None:
        """Rotate one stopped OSD's key and write it to the keyring and the label

        The new key is held in a shell variable on the target host and is never
        returned to the cookbook, so that it stays out of the logs. Both copies are
        verified on the host by comparing values there.
        """
        data_dir = f"/var/lib/ceph/osd/ceph-{osd_id}"
        keyring = f"{SCRATCH_DIR}/osd.{osd_id}.keyring"
        # Chained with && so that a failure stops immediately, leaving the OSD down
        # for inspection rather than starting it with a key the cluster has replaced.
        command = " && ".join(
            [
                "umask 077",
                f"install -d -m 0700 {SCRATCH_DIR}",
                f"ceph auth rotate --key-type={KEY_TYPE} osd.{osd_id} > {keyring}",
                f"ceph-authtool {data_dir}/keyring --import-keyring {keyring}",
                f"chown ceph:ceph {data_dir}/keyring",
                f"chmod 0600 {data_dir}/keyring",
                f"key=$(ceph auth get-key osd.{osd_id})",
                f'ceph-bluestore-tool set-label-key --dev {device} -k {LABEL_KEY} -v "$key"',
                # Verify both copies without printing the key anywhere
                f'test "$(ceph-authtool {data_dir}/keyring -n osd.{osd_id} -p)" = "$key"',
                f'test "$(ceph-bluestore-tool show-label --dev {device} | jq -r \'.[].{LABEL_KEY}\')" = "$key"',
                f"rm -f {keyring}",
            ]
        )
        hosts.run_sync(command)

    def _selected_osds(self, hosts: RemoteHosts, devices: dict[str, str]) -> list[str]:
        """Return the OSD ids to rotate on this host, in numeric order"""
        candidates = sorted(devices, key=int)
        if self._args.osd:
            missing = set(self._args.osd) - set(candidates)
            if missing:
                raise RuntimeError(
                    f"OSDs not present on {hosts}: {' '.join(sorted(missing))}"
                )
            candidates = [osd for osd in candidates if osd in set(self._args.osd)]

        if self._args.force:
            return candidates

        insecure = self._insecure_osds(hosts)
        skipped = [osd for osd in candidates if osd not in insecure]
        if skipped:
            logger.info(
                "%s: skipping %d OSD(s) already using a secure key type: %s",
                hosts,
                len(skipped),
                " ".join(skipped),
            )
        return [osd for osd in candidates if osd in insecure]

    def _insecure_osds(self, hosts: RemoteHosts) -> set[str]:
        """Return the ids of OSDs the cluster reports as using an insecure key type"""
        messages = self._capture(
            hosts,
            "ceph health detail --format json "
            "| jq -r '.checks.AUTH_INSECURE_SERVICE_KEY_TYPE.detail[]?.message // empty'",
        )
        insecure = set()
        for line in messages.splitlines():
            # "entity osd.12 using insecure key type: aes"
            fields = line.split()
            if len(fields) < 2 or not fields[1].startswith("osd."):
                continue
            osd_id = fields[1].split(".", 1)[1]
            # The clusters also carry unused osd.$HOSTNAME entities, which are not
            # OSDs and are rotated through Puppet rather than here. Real OSD ids are
            # always decimal.
            if osd_id.isdigit():
                insecure.add(osd_id)
        return insecure

    def _osd_devices(self, hosts: RemoteHosts) -> dict[str, str]:
        """Return a map of OSD id to the logical volume holding its BlueStore label

        `ceph-volume lvm list` is keyed by OSD id and reports the `lv_path`, which is
        the name to use. The data directory's `block` symlink resolves instead to a
        device-mapper node whose number is not stable across reboots, and
        `ceph-volume raw list` is keyed by OSD fsid rather than id.

        Every Ceph cluster we run is LVM-backed. An OSD provisioned some other way
        would not appear here, so fail rather than silently skip it.
        """
        lvm = json.loads(self._capture(hosts, "ceph-volume lvm list --format json"))
        if not lvm:
            raise RuntimeError(
                f"ceph-volume lvm list reported no OSDs on {hosts}. If these OSDs were "
                "not provisioned with LVM, this cookbook needs extending to read "
                "ceph-volume raw list."
            )

        devices = {}
        for osd_id, entries in lvm.items():
            block = [entry for entry in entries if entry.get("type") == "block"]
            if not block:
                raise RuntimeError(f"osd.{osd_id} on {hosts} has no block device in ceph-volume lvm list")
            devices[osd_id] = block[0]["lv_path"]
        return devices

    def _capture(self, hosts: RemoteHosts, command: str) -> str:
        """Run a read-only command on a single host and return its output"""
        for _, output in hosts.run_sync(
            command, is_safe=True, print_output=False, print_progress_bars=False
        ):
            return output.message().decode()
        raise RuntimeError(f"No output from {command} on {hosts}")

    @retry(
        tries=60,
        delay=timedelta(seconds=30),
        backoff_mode="constant",
        exceptions=(RemoteExecutionError,),
    )
    def _wait_for(self, hosts: RemoteHosts, command: str) -> None:
        """Retry a command until it exits zero, or give up after 30 minutes"""
        hosts.run_sync(command, print_progress_bars=False)
