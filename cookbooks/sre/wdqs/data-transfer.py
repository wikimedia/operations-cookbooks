"""WDQS data transfer cookbook for source node

Usage example:
    cookbook sre.wdqs.data-transfer --source wdqs1004.eqiad.wmnet --dest wdqs1010.eqiad.wmnet
     --reason "allocator troubles" --blazegraph-instance blazegraph --task-id T12345

"""
import argparse
import logging
import string
import threading

from datetime import timedelta
from random import SystemRandom
from time import sleep

BLAZEGRAPH_INSTANCES = {
    'categories': {
        'services': ['wdqs-categories'],
        'files': ['/srv/wdqs/categories.jnl'],
    },
    'blazegraph': {
        'services': ['wdqs-updater', 'wdqs-blazegraph'],
        'files': ['/srv/wdqs/wikidata.jnl', '/srv/wdqs/aliases.map'],
    }
}

__title__ = "WDQS data transfer cookbook"
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """Parse the command line arguments for all the sre.elasticsearch cookbooks."""
    parser = argparse.ArgumentParser(prog=__name__, description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--source', required=True, help='FQDN of source node.')
    parser.add_argument('--dest', required=True, help='FQDN of destination node.')
    parser.add_argument('--blazegraph_instance', required=True, choices=list(BLAZEGRAPH_INSTANCES.keys()),
                        help='One of: %(choices)s.')
    parser.add_argument('--reason', required=True, help='Administrative Reason')
    parser.add_argument('--downtime', type=int, default=6, help="Hours of downtime")
    parser.add_argument('--task-id', help='task_id for the change')

    return parser


def _copy_file(source, dest, file):
    """Copy file from one node to the other via netcat."""
    passwd = _generate_pass()
    port = 9876
    recv_cmd = "nc -l -p {port} | openssl enc -d -aes-256-cbc -k {passwd} | pigz -c -d > {file}".format(
        port=port, file=file, passwd=passwd)
    send_cmd = "pigz -c {file} | openssl enc -e -aes-256-cbc -k {passwd} | nc -w 3 {dest} {port}".format(
        file=file, dest=dest.hosts, passwd=passwd, port=port)

    send = threading.Thread(target=source.run_sync, args=(recv_cmd,))
    receive = threading.Thread(target=dest.run_sync, args=(send_cmd,))

    receive.start()
    send.start()

    receive.join()
    send.join()


def _generate_pass():
    """Generate a random string of fixed length."""
    sysrand = SystemRandom()
    passwd_charset = string.ascii_letters + string.digits
    return ''.join([sysrand.choice(passwd_charset) for _ in range(32)])


def run(args, spicerack):
    """Required by Spicerack API."""
    remote_hosts = spicerack.remote().query("{source},{dest}".format(source=args.source, dest=args.dest))
    icinga = spicerack.icinga()
    puppet = spicerack.puppet(remote_hosts)
    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    source = spicerack.remote().query(args.source)
    dest = spicerack.remote().query(args.dest)

    if len(source) != 1:
        raise ValueError("Only one node is needed. Not {total}({source})".
                         format(total=len(source), source=source))

    if len(dest) != 1:
        raise ValueError("Only one destination node is needed. Not {total}({source})".
                         format(total=len(source), source=source))

    services = BLAZEGRAPH_INSTANCES[args.blazegraph_instance]['services']
    files = BLAZEGRAPH_INSTANCES[args.blazegraph_instance]['files']

    stop_services_cmd = " && ".join(["systemctl stop " + service for service in services])
    services.reverse()
    start_services_cmd = " && sleep 10 && ".join(["systemctl start " + service for service in services])

    with icinga.hosts_downtimed(remote_hosts.hosts, reason, duration=timedelta(hours=args.downtime)):
        with puppet.disabled(reason):
            remote_hosts.run_sync('depool')
            sleep(180)
            remote_hosts.run_sync(stop_services_cmd)

            for file in files:
                _copy_file(source, dest, file)
                dest.run_sync('chown blazegraph: "{file}"'.format(file=file))

            if args.blazegraph_instance == 'blazegraph':
                dest.run_sync('touch /srv/wdqs/data_loaded')

            remote_hosts.run_sync(start_services_cmd)
            remote_hosts.run_sync('pool')
