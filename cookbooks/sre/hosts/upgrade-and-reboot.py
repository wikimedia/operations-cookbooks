r"""Upgrade all packages on a host and reboot it.

- Set Icinga downtime
- Disable puppet
- Depool
- Wait for drain
- Upgrade software
- Reboot
- Wait for host to come back online
- Force a puppet run
- Repool
- Remove Icinga downtime

Usage example:
    cookbook sre.hosts.upgrade-and-reboot lvs2002.codfw.wmnet --depool-cmd="systemctl stop pybal" \
        --repool-cmd="systemctl start pybal"
    cookbook sre.hosts.upgrade-and-reboot cp3030.esams.wmnet --depool-cmd="depool" --repool-cmd="pool"

"""
import argparse
import logging
import time

from datetime import datetime, timedelta


__title__ = 'Upgrade all packages on a host and reboot it.'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('host', help='FQDN of the host to act upon.')
    parser.add_argument('--depool-cmd', required=True,
                        help='Command used to depool the service (eg: "service pybal stop").')
    parser.add_argument('--repool-cmd', required=True, help='Command used to repool the service (eg: "pool").')
    parser.add_argument('--sleep', type=int, default=60,
                        help='Sleep in seconds to wait after the depool before proceeding. [optional, default=60]')
    parser.add_argument('-S', '--use-sudo', required=False, action="store_true",
                        help='If set will use sudo when sshing to the host.')

    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    remote_host = spicerack.remote().query(args.host, use_sudo=args.use_sudo)
    icinga = spicerack.icinga()
    puppet = spicerack.puppet(remote_host)
    reason = spicerack.admin_reason('Software upgrade and reboot')

    icinga.downtime_hosts(remote_host.hosts, reason, duration=timedelta(minutes=20))
    puppet.disable(reason)

    # Depool and wait a bit for the host to be drained
    if args.depool_cmd:
        remote_host.run_sync(args.depool_cmd)
    else:
        logging.info('Not performing any depool action as requested (empty --depool-cmd)')

    logging.info('Waiting for %s to be drained.', args.host)
    time.sleep(args.sleep)

    # Upgrade all packages, leave config files untouched, do not prompt
    upgrade_cmd = ("DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::='--force-confdef' "
                   "-o Dpkg::Options::='--force-confold' dist-upgrade")
    remote_host.run_sync(upgrade_cmd)

    reboot_time = datetime.utcnow()
    remote_host.reboot()
    remote_host.wait_reboot_since(reboot_time)
    puppet.run(enable_reason=reason)

    # Repool
    if args.repool_cmd:
        remote_host.run_sync(args.repool_cmd)
    else:
        logging.info('Not performing any repool action as requested (empty --repool-cmd)')

    icinga.remove_downtime(remote_host.hosts)
