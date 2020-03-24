"""Update and deploy the DNS records generated from Netbox data.

Run the script that generates the DNS zonefile snippets on the Netbox host to update the exposed git repository with
the snippets and then deploy them to the authdns hosts, reloading gdnsd.

Usage example:
    cookbook sre.dns.netbox -t T12345 'Decommissioned mw12[22-35]'

"""
import argparse
import json
import logging

from cumin.transports import Command

from spicerack.interactive import ask_confirmation


__title__ = 'Update and deploy the DNS records generated from Netbox'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name
NETBOX_DOMAIN = 'netbox.wikimedia.org'
NETBOX_BARE_REPO_PATH = '/srv/netbox-exports/dns.git'
NETBOX_USER = 'netbox'
NETBOX_HOSTS_QUERY = 'A:netbox'
AUTHDNS_NETBOX_CHECKOUT_PATH = '/srv/git/netbox_dns_snippets'
AUTHDNS_USER = 'netboxdns'
AUTHDNS_HOSTS_QUERY = 'A:dns-auth'
AUTHDNS_DNS_CHECKOUT_PATH = '/srv/authdns/git'


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-t', '--task-id', help='The Phabricator task ID (e.g. T12345).')
    parser.add_argument('message', help='Commit message')

    return parser


def run(args, spicerack):  # pylint: disable=too-many-locals
    """Required by Spicerack API."""
    remote = spicerack.remote()
    netbox_hostname = spicerack.dns().resolve_cname(NETBOX_DOMAIN)
    netbox_host = remote.query(netbox_hostname)
    netbox_hosts = remote.query(NETBOX_HOSTS_QUERY)
    reason = spicerack.admin_reason(args.message, task_id=args.task_id)
    # Always set an accessible CWD for runuser because the Python git module passes it to Popen
    base_command = ('cd /tmp && runuser -u {user} -- python3 '
                    '/srv/deployment/netbox-extras/dns/generate_dns_snippets.py').format(user=NETBOX_USER)

    command_str = ('{base} commit --batch "{owner}: {msg}"').format(
        base=base_command, owner=reason.owner, msg=args.message)
    # NO_CHANGES_RETURN_CODE = 99 in generate_dns_snippets.py
    command = Command(command_str, ok_codes=[0, 99])

    logger.info('Generating the DNS records from Netbox data. It will take a couple of minutes.')
    results = netbox_host.run_sync(command)
    metadata = {}
    for _, output in results:
        lines = output.message().decode()
        logger.info(lines)
        for line in lines.splitlines():
            if line.startswith('METADATA:'):
                metadata = json.loads(line.split(maxsplit=1)[1])
                break

    if metadata.get('no_changes', False):
        logger.info('No changes to deploy.')
        return

    ask_confirmation('Have you checked that the diff is OK?')

    command = ('{base} push "{path}" "{sha1}"').format(
        base=base_command, path=metadata.get('path', ''), sha1=metadata.get('sha1', ''))
    results = netbox_host.run_sync(command)
    for _, output in results:
        logger.info(output.message().decode())

    passive_netbox_hosts = remote.query(str(netbox_hosts.hosts - netbox_host.hosts))
    logger.info('Updating the Netbox passive copies of the repository on %s', passive_netbox_hosts)
    passive_netbox_hosts.run_sync('cd {path} && runuser -u {user} -- git fetch {host} master:master'.format(
        path=NETBOX_BARE_REPO_PATH, user=NETBOX_USER, host=netbox_hostname))

    authdns_hosts = remote.query(AUTHDNS_HOSTS_QUERY)
    logger.info('Updating the authdns copies of the repository on %s', authdns_hosts)
    authdns_hosts.run_sync('cd {path} && runuser -u {user} -- git pull'.format(
        path=AUTHDNS_NETBOX_CHECKOUT_PATH, user=AUTHDNS_USER))

    logger.info('Deploying the updated zonefiles on %s', authdns_hosts)
    authdns_hosts.run_sync('cd {git} && utils/deploy-check.py -g {netbox} --deploy'.format(
        git=AUTHDNS_DNS_CHECKOUT_PATH, netbox=AUTHDNS_NETBOX_CHECKOUT_PATH))
