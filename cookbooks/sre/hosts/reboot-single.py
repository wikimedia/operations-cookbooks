r"""Downtime a single host and reboot it

- Set Icinga downtime
- Reboot
- Wait for host to come back online
- Remove the Icinga downtime after the host has been rebooted and the
  first Puppet run is complete

This is meant for one off servers and doesn't support pooling/depooling
clustered services (yet).

Usage example:
    cookbook sre.hosts.reboot-single sretest1001.eqiad.wmnet

"""
import argparse
import logging
import time

from datetime import datetime, timedelta

from spicerack.decorators import retry
from spicerack.icinga import IcingaError

__title__ = 'Downtime a single host and reboot it'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """Parse arguments"""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('host', help='A single host to be rebooted (specified in Cumin query syntax)')
    parser.add_argument('--depool', help='Wether to run depool/pool on the server around reboots.',
                        action='store_true')
    return parser


@retry(tries=20, delay=timedelta(seconds=3), backoff_mode='linear', exceptions=(IcingaError,))
def wait_for_icinga_optimal(icinga, remote_host):
    """Waits for an icinga optimal status, else raises an exception."""
    status = icinga.get_status(remote_host.hosts)
    if not status.optimal:
        failed = ["{}:{}".format(k, ','.join(v)) for k, v in status.failed_services.items()]
        raise IcingaError('Not all services are recovered: {}'.format(' '.join(failed)))


def run(args, spicerack):
    """Reboot the host"""
    remote_host = spicerack.remote().query(args.host)

    if len(remote_host) == 0:
        logger.error('Specified server not found, bailing out')
        return 1

    if len(remote_host) != 1:
        logger.error('Only a single server can be rebooted')
        return 1

    icinga = spicerack.icinga()
    puppet = spicerack.puppet(remote_host)
    reason = spicerack.admin_reason('Rebooting host')

    with icinga.hosts_downtimed(remote_host.hosts, reason, duration=timedelta(minutes=20)):
        if args.depool:
            remote_host.run_async('depool')
            logger.info('Waiting a 30 second grace period after depooling')
            time.sleep(30)
        reboot_time = datetime.utcnow()
        remote_host.reboot()
        remote_host.wait_reboot_since(reboot_time)
        puppet.wait_since(reboot_time)

        # First let's try to check if icinga is already in optimal state.
        # If not, we require a recheck all service, then
        # wait a grace period before declaring defeat.
        icinga_ok = icinga.get_status(remote_host.hosts).optimal
        if not icinga_ok:
            icinga.recheck_all_services(remote_host.hosts)
            try:
                wait_for_icinga_optimal(icinga, remote_host)
                icinga_ok = True
            except IcingaError as e:
                logger.error(str(e))

        if args.depool:
            if icinga_ok:
                remote_host.run_async('pool')
            else:
                logger.warning("NOT repooling the services.")

        # Return an error if icinga didn't recover.
        if not icinga_ok:
            return 1
