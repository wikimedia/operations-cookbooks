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
from wmflib.interactive import ask_confirmation, confirm_on_failure


__title__ = 'Update and deploy the DNS records generated from Netbox'
logger = logging.getLogger(__name__)
NETBOX_BARE_REPO_PATH = '/srv/netbox-exports/dns.git'
NETBOX_USER = 'netbox'
NETBOX_HOSTS_QUERY = 'netbox[1-2]00*.wmnet'
AUTHDNS_NETBOX_CHECKOUT_PATH = '/srv/git/netbox_dns_snippets'
AUTHDNS_USER = 'netboxdns'
AUTHDNS_HOSTS_QUERY = 'A:dns-auth'
AUTHDNS_DNS_CHECKOUT_PATH = '/srv/authdns/git'


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-t', '--task-id', help='The Phabricator task ID (e.g. T12345).')
    parser.add_argument('--force', metavar='SHA1',
                        help=('Continue on no changes to force the replication to the other Netbox host(s) and the '
                              'push to the authoritative DNS hosts of the SHA1 given as parameter. Has no effect if '
                              'there are changes. For more details, see '
                              'https://wikitech.wikimedia.org/wiki/DNS/Netbox#Force_update_generated_records'))
    parser.add_argument('--skip-authdns-update', action='store_true',
                        help=('Do not perform an authdns-update after having pushed the changes to the local checkout '
                              'of the Netbox-generated repository in all the dns-auth hosts. This allows to stage '
                              'changes that require also a commit in the manual repository. After running this '
                              'cookbook a manual authdns-update will pick up also these changes.'))
    parser.add_argument('--emergency-manual-edit', action='store_true',
                        help=('CAUTION: to be used only in an emergency! Allow to edit the files manually before '
                              'committing them. Be aware that any subsequent run of the cookbook will try to revert '
                              'the manual modifications.'))
    parser.add_argument('message', help='Commit message')

    return parser


def run(args, spicerack):  # pylint: disable=too-many-locals
    """Required by Spicerack API."""
    remote = spicerack.remote()
    netbox_host = spicerack.netbox_master_host
    netbox_hostname = str(netbox_host)
    netbox_hosts = remote.query(NETBOX_HOSTS_QUERY)
    reason = spicerack.admin_reason(args.message, task_id=args.task_id)
    # Always set an accessible CWD for runuser because the Python git module passes it to Popen
    base_command = ('cd /tmp && runuser -u {user} -- python3 '
                    '/srv/deployment/netbox-extras/dns/generate_dns_snippets.py').format(user=NETBOX_USER)

    extra_options = ''
    if args.emergency_manual_edit:
        extra_options = '--keep-files '
    command_str = ('{base} commit {opts}--batch "{owner}: {msg}"').format(
        opts=extra_options, base=base_command, owner=reason.owner, msg=args.message)
    # NO_CHANGES_RETURN_CODE = 99 in generate_dns_snippets.py
    command = Command(command_str, ok_codes=[0, 99])

    logger.info('Generating the DNS records from Netbox data. It will take a couple of minutes.')
    results = netbox_host.run_sync(command, is_safe=True)
    metadata = {}
    for _, output in results:
        lines = output.message().decode()
        for line in lines.splitlines():
            if line.startswith('METADATA:'):
                metadata = json.loads(line.split(maxsplit=1)[1])
                break

    if spicerack.dry_run:
        if not metadata.get('no_changes', False):
            logger.info('Bailing out in DRY-RUN mode. Generated temporary files are available on %s:%s',
                        netbox_hostname, metadata.get('path'))
        return

    if args.emergency_manual_edit:
        logger.info('Generated temporary files are available on %s:%s', netbox_hostname, metadata.get('path'))
        logger.info('SSH there, as root modify any file, git stage them and run "git commit --amend" to commit them')
        logger.info('Then run "git log --pretty=oneline -1" and copy the new SHA1 of HEAD')
        metadata['sha1'] = input('Enter the new SHA1 of the commit to push: ')
        metadata['no_changes'] = False

    if metadata.get('no_changes', False):
        if args.force:
            logger.info('No changes to deploy but --force set to %s, continuing.', args.force)
            sha1 = args.force
        else:
            logger.info('No changes to deploy.')
            return
    else:
        ask_confirmation('Have you checked that the diff is OK?')

        sha1 = metadata.get('sha1', '')
        if not sha1:
            raise RuntimeError('Unable to fetch SHA1 from commit metadata: {meta}'.format(meta=metadata))

        command = ('{base} push "{path}" "{sha1}"').format(base=base_command, path=metadata.get('path', ''), sha1=sha1)
        results = netbox_host.run_sync(command)

    passive_netbox_hosts = remote.query(str(netbox_hosts.hosts - netbox_host.hosts))
    logger.info('Updating the Netbox passive copies of the repository on %s', passive_netbox_hosts)
    passive_netbox_hosts.run_sync('runuser -u {user} -- git -C "{path}" fetch {host} master:master'.format(
        path=NETBOX_BARE_REPO_PATH, user=NETBOX_USER, host=netbox_hostname))

    authdns_hosts = remote.query(AUTHDNS_HOSTS_QUERY)
    logger.info('Updating the authdns copies of the repository on %s', authdns_hosts)
    confirm_on_failure(
        authdns_hosts.run_sync,
        'runuser -u {user} -- git -C "{path}" fetch && git -C "{path}" merge --ff-only {sha1}'.format(
            path=AUTHDNS_NETBOX_CHECKOUT_PATH, user=AUTHDNS_USER, sha1=sha1))

    if args.skip_authdns_update:
        logger.warning(('ATTENTION! Skipping deploy of the updated zonefiles. The next manual authdns-update or '
                        'run of this cookbook will deploy the changes!'))
    else:
        logger.info('Deploying the updated zonefiles on %s', authdns_hosts)
        confirm_on_failure(
            authdns_hosts.run_sync,
            'cd {git} && utils/deploy-check.py -g {netbox} --deploy'.format(
                git=AUTHDNS_DNS_CHECKOUT_PATH, netbox=AUTHDNS_NETBOX_CHECKOUT_PATH))
    spicerack.run_cookbook('sre.puppet.sync-netbox-hiera', [f'Triggered by {__name__}: {reason.reason}'])
