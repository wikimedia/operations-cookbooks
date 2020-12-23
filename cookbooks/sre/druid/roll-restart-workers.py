"""Restart all Druid jvm-base daemons in a cluster

Every Druid worker host manages multiple daemons:
* Historical
* Broker
* MiddleManager
* Overlord
* Coordinator

All of them are heavily relying on Zookeeper, that is currently
co-located in the same cluster (but not part of this cookbook).

Upstream suggests to restart one daemon at the time when restarting
or upgrading, the order is not extremely important. The longest and
more delicate restart is the Historical's, since the daemon needs
to load Druid segments from cache/HDFS and it might take minutes to
complete. Up to now we rely on tailing logs on the host to establish
when all the segments are loaded (and make sure that everything looks
ok), but in the future we'll probably rely on a metric to have a more
automated cookbook.

"""
import argparse
import logging

from datetime import timedelta

from wmflib.interactive import ensure_shell_is_durable

from cookbooks import ArgparseFormatter


__title__ = 'Roll restart all the jvm daemons on Druid worker nodes'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('cluster', help='The name of the Druid cluster to work on.',
                        choices=['public', 'analytics'])
    parser.add_argument('--daemons', help='The daemons to restart.', nargs='+',
                        default=['historical', 'overlord', 'middlemanager', 'broker', 'coordinator'],
                        choices=['historical', 'overlord', 'middlemanager', 'broker', 'coordinator'])
    return parser


def run(args, spicerack):
    """Restart all Druid jvm daemons on a given cluster"""
    cluster_cumin_alias = "A:druid-" + args.cluster

    need_depool = False
    if args.cluster == 'public':
        need_depool = True

    ensure_shell_is_durable()
    druid_workers = spicerack.remote().query(cluster_cumin_alias)
    icinga = spicerack.icinga()
    reason = spicerack.admin_reason('Roll restart of Druid\'s jvm daemons.')

    with icinga.hosts_downtimed(druid_workers.hosts, reason,
                                duration=timedelta(minutes=60)):

        logger.info(
            'Restarting daemons (%s), one host at the time.', ','.join(args.daemons))
        commands = []
        for daemon in args.daemons:
            commands.append('systemctl restart druid-' + daemon)
            if daemon == 'overlord':
                commands.append('sleep 300')
            else:
                commands.append('sleep 30')

        if need_depool:
            commands = ['depool', 'sleep 60'] + commands + ['pool']

        druid_workers.run_async(*commands, batch_size=1, batch_sleep=120.0)

    logger.info("All Druid jvm restarts completed!")
