"""Update and deploy the DNS records generated from Netbox data.

Run the script that generates the DNS zonefile snippets on the Netbox host to update the exposed git repository with
the snippets and then deploy them to the authdns hosts, reloading gdnsd.

Usage example:
    cookbook sre.dns.netbox 'Decommissioned mw12[22-35] - T12345'

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
AUTHDNS_CHECKOUT_PATH = '/srv/git/netbox_dns_snippets'
AUTHDNS_USER = 'netboxdns'
AUTHDNS_HOSTS_QUERY = 'A:dns-auth'


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-t', '--task-id', help='The Phabricator task ID (e.g. T12345).')
    parser.add_argument('message', help='Commit message')

    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    remote = spicerack.remote()
    netbox_hostname = spicerack.dns().resolve_cname(NETBOX_DOMAIN)
    netbox_host = remote.query(netbox_hostname)
    netbox_hosts = remote.query(NETBOX_HOSTS_QUERY)
    reason = spicerack.admin_reason(args.message, task_id=args.task_id)
    # Always set an accessible CWD for runuser because the Python git module passes it to Popen
    base_command = ('cd /tmp && runuser -u {user} python3 '
                    '/srv/deployment/netbox-extras/dns/generate_dns_snippets.py').format(user=NETBOX_USER)

    command_str = ('{base} commit --batch "{owner}: {msg}"').format(
        base=base_command, owner=reason.owner, msg=args.message)
    # NO_CHANGES_RETURN_CODE = 99 in generate_dns_snippets.py
    command = Command(command_str, ok_codes=[0, 99])

    results = netbox_host.run_sync(command)
    line = '{}'
    for _, output in results:
        line = output.message().decode()
        logger.info(line)

    # Last line contains the metadata
    metadata = json.loads(line)

    ask_confirmation('Have you checked that the diff is OK?')

    command = ('{base} push "{path}" "{sha1}"').format(
        base=base_command, path=metadata.get('path', ''), sha1=metadata.get('sha1', ''))
    results = netbox_host.run_sync(command)
    for _, output in results:
        logger.info(output.message().decode())

    passive_netbox_hosts = remote.query(str(netbox_hosts.hosts - netbox_host.hosts))
    passive_netbox_hosts.run_sync('cd {path} && runuser -u {user} git fetch {host} master:master'.format(
        path=NETBOX_BARE_REPO_PATH, user=NETBOX_USER, host=netbox_hostname))
    remote.query(AUTHDNS_HOSTS_QUERY).run_sync('cd {path} && runuser -u {user} git pull'.format(
        path=AUTHDNS_CHECKOUT_PATH, user=AUTHDNS_USER))

    # TODO: add the gdnsd deploy/reload once actually used in production
