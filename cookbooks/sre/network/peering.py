"""Manage IX peering sessions"""

import logging
import smtplib
from collections import defaultdict
from typing import Union

from email.message import EmailMessage
from ipaddress import ip_address

from prettytable import PrettyTable

from spicerack.cookbook import CookbookBase, CookbookInitSuccess, CookbookRunnerBase
from wmflib.interactive import ask_confirmation

from cookbooks.sre.network import get_junos_bgp_summary, run_junos_commands

logger = logging.getLogger(__name__)

WIKIMEDIA_ASN = 14907
DEFAULT_MAX_PREFIX4 = 10000
DEFAULT_MAX_PREFIX6 = 4000

FOOTER = """
The Wikimedia Foundation operates some of the largest collaboratively
edited reference projects in the world, including Wikipedia. We are a
501(c)3 nonprofit charitable organization dedicated to encouraging the
growth, development and distribution of free, multilingual content, and
to providing the full content of these wiki-based projects to the public
free of charge.

You can find out more about us and our mission at
https://wikimediafoundation.org/
and our peering policy and information at:
http://as14907.peeringdb.com/
http://wikimediafoundation.org/wiki/Peering

Thanks for peering,
--
The Wikimedia Netops team
"""


class Peering(CookbookBase):
    """Manage IX peering sessions

    Cookbook for day to day peer management. For a given peer AS:
     - show: bird eye view of all existing and possible sessions
     - configure: add missing sessions
     - clear: reset Idle sessions (usually due to prefix limit violation)

    Script is idempotent and prompts the user before any intrusive change.
    Caches PeeringDB requests by default.
    Use with `--dry-run` to not !log the read only actions

    Usage example:
        cookbook -d sre.network.peering show 293
        cookbook sre.network.peering --no-cache configure 293
        cookbook sre.network.peering clear 293
        cookbook -d sre.network.peering email 293

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('action', help='Action to perform', choices=['show', 'configure', 'clear', 'email'])
        parser.add_argument('ASN', help='Peer ASN', type=int)
        parser.add_argument('--no-cache', action='store_true', help='Bypass local PeeringDB cache.')
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return PeeringRunner(args, self.spicerack)


class PeeringRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the peering runner."""
        self.args = args
        self.pdb = spicerack.peeringdb(ttl=0) if args.no_cache else spicerack.peeringdb()
        self.netbox = spicerack.netbox()
        self.remote = spicerack.remote()
        self.routers_bgp = {}
        self.dry_run = spicerack.dry_run
        if self.args.ASN == WIKIMEDIA_ASN:
            raise RuntimeError("That's our AS number...")
        # Action "show" is here so it doesn't !log it
        if self.args.action == 'show':
            print(self.peering_matrix(WIKIMEDIA_ASN, self.args.ASN))
            raise CookbookInitSuccess()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"with action '{self.args.action}' for AS: {self.args.ASN}"

    def run(self):
        """Main entry point, as required by Spicerack API."""
        if self.args.action == 'email':
            self.email(WIKIMEDIA_ASN, self.args.ASN)
            return
        print(self.peering_matrix(WIKIMEDIA_ASN, self.args.ASN))
        if self.args.action == 'configure':
            self.configure_junos(WIKIMEDIA_ASN, self.args.ASN)
        elif self.args.action == 'clear':
            self.clear_sessions(WIKIMEDIA_ASN, self.args.ASN)

    def netixlan_net(self, data: dict) -> dict:
        """Extracts relevant fields from a peer's IX presences"""
        data_net = defaultdict(list)
        for netixlan in data['netixlan_set']:
            if not netixlan['operational'] or netixlan['status'] != 'ok':
                # Ignore IX presence not ready yet
                continue
            ixlan = netixlan['ixlan_id']
            to_copy = ['asn', 'ipaddr4', 'ipaddr6']
            net = {i: netixlan[i] for i in to_copy if i in netixlan}
            data_net[ixlan].append(net)
        return data_net

    def ixname(self, ixlan_id: int) -> str:
        """Returns the name of an IXP based on its ID"""
        for ixlan in self.pdb.fetch('ixlan'):
            if ixlan['id'] == ixlan_id:
                ix_id = ixlan['ix_id']
                break
        ix_name = self.pdb.fetch('ix', ix_id)[0]['name']

        return ix_name

    def get_device_fqdn_from_ip(self, address: str) -> str:
        """Returns the FQDN of a device associated to an IP"""
        return self.netbox.api.ipam.ip_addresses.get(address=address).assigned_object.device.primary_ip.dns_name

    def format_sessions(self, asn_l: int, asn_r: int) -> list:
        """Massage PeeringDB data to a list of BGP sessions between networks"""
        sessions = []
        data_l = self.pdb.fetch_asn(asn_l)[0]
        data_r = self.pdb.fetch_asn(asn_r)[0]
        netixlan_l = self.netixlan_net(data_l)
        netixlan_r = self.netixlan_net(data_r)
        common = sorted(set(netixlan_l.keys()).intersection(set(netixlan_r.keys())))
        for ixlan in common:
            for i in netixlan_l[ixlan]:
                for k in netixlan_r[ixlan]:
                    for afi in ['ipaddr4', 'ipaddr6']:
                        if i[afi] is None or k[afi] is None:
                            continue
                        session: dict = {}
                        session['ix_name'] = self.ixname(ixlan)
                        session['name_l'] = data_l['name']
                        session['asn_l'] = asn_l
                        session['name_r'] = data_r['name']
                        session['asn_r'] = asn_r
                        session['ip_l'] = ip_address(i[afi])
                        session['ip_r'] = ip_address(k[afi])
                        session['router_fqdn'] = self.get_device_fqdn_from_ip(i[afi])
                        router_peer = self.routers_bgp_table(session['router_fqdn'], session['ip_r'])
                        session['status'] = router_peer['peer-state'] if router_peer else 'Not configured'
                        session['uptime'] = router_peer['elapsed-time'] if router_peer else ''
                        session['max_prefixes'] = data_r['info_prefixes' + afi[6:]]
                        sessions.append(session)
        return sessions

    # TODO typehint of peer_address
    def routers_bgp_table(self, router_fqdn: str, peer_address=None) -> dict:
        """Wrapper function to lazy load router BGP summary tables"""
        if router_fqdn in self.routers_bgp:
            bgp_summary = self.routers_bgp[router_fqdn]
        else:
            remote_host = self.remote.query('D{' + router_fqdn + '}')
            bgp_summary = get_junos_bgp_summary(remote_host)
            self.routers_bgp[router_fqdn] = bgp_summary
        if peer_address:
            return bgp_summary[peer_address] if peer_address in bgp_summary else {}
        return bgp_summary

    def peering_matrix(self, asn_l: int, asn_r: int) -> Union[PrettyTable, str]:
        """Nicely display possible and existing BGP sessions between networks"""
        sessions = self.format_sessions(asn_l, asn_r)
        if not sessions:
            logger.info('No IXP in common with AS%s', asn_r)
            return ''
        name_r = sessions[0]['name_r']
        # TODO currently assumes that the peer use the same AS# at each IXP
        # We could check if that's the case or not by iterating once over `sessions`
        table = PrettyTable(['IXP', f'Wikimedia AS{asn_l}', f'{name_r} AS{asn_r}', 'Status'])
        for session in sessions:
            if session['status'] not in ('Established', 'Not configured'):
                status = f"Down since {session['uptime']}"
            else:
                status = session['status']
            table.add_row([session['ix_name'],
                           session['ip_l'],
                           session['ip_r'],
                           status])
        return table

    def configure_junos(self, asn_l: int, asn_r: int) -> None:
        """Configure BGP sessions between networks"""
        all_commands = {}
        sessions = self.format_sessions(asn_l, asn_r)
        # TODO: handle removing of sessions
        # TODO: handle prefix limit
        for session in sessions:
            # Ignore configured sessions
            if session['status'] != 'Not configured':
                continue
            peer_ip = session['ip_r']
            logger.info('To be configured on %s: %s', session['router_fqdn'], peer_ip)
            commands = []
            commands.append((f"set protocols bgp group {'IX' + str(peer_ip.version)}"
                             f" neighbor {peer_ip} peer-as {session['asn_r']}"))
            commands.append(f"set protocols bgp group {'IX' + str(peer_ip.version)}"
                            f" neighbor {peer_ip} description \"{session['name_r']}\"")
            if session['max_prefixes'] > globals()['DEFAULT_MAX_PREFIX' + str(peer_ip.version)]:
                inet = 'inet' if peer_ip.version == 4 else 'inet6'
                commands.append((f"set protocols bgp group {'IX' + str(peer_ip.version)} neighbor {peer_ip}"
                                 f" family {inet} unicast prefix-limit maximum {session['max_prefixes']}"))
                commands.append((f"set protocols bgp group {'IX' + str(peer_ip.version)} neighbor {peer_ip}"
                                 f" family {inet} unicast prefix-limit teardown idle-timeout forever"))
                commands.append((f"set protocols bgp group {'IX' + str(peer_ip.version)} neighbor {peer_ip}"
                                 f" family {inet} unicast prefix-limit teardown 80"))

            if not session['router_fqdn'] in all_commands:
                all_commands[session['router_fqdn']] = commands
            else:
                all_commands[session['router_fqdn']].extend(commands)

        if not all_commands:
            logger.info('Nothing to configure')
            return
        for router, commands in all_commands.items():
            logger.info('Configuring peers on %s', router)
            remote_host = self.remote.query('D{' + router + '}')
            run_junos_commands(remote_host, commands)

    def clear_sessions(self, asn_l: int, asn_r: int) -> None:
        """Clear Idle BGP sessions"""
        sessions = self.format_sessions(asn_l, asn_r)
        for session in sessions:
            # Ignore configured sessions
            if session['status'] != 'Idle':
                continue
            peer_ip = session['ip_r']
            ask_confirmation(f"Clear BGP session on {session['router_fqdn']}: {peer_ip}?")
            remote_host = self.remote.query('D{' + session['router_fqdn'] + '}')
            remote_host.run_sync(f"clear bgp neighbor {peer_ip}",
                                 print_output=False,
                                 print_progress_bars=False)

    def contact_emails(self, asn: int, role: str) -> set:
        """Returns one or more emails matching a role"""
        data = self.pdb.fetch_asn(asn)[0]
        contact_emails = set()
        all_emails = set()
        found = False
        if not data['poc_set']:
            logger.error('No point of contacts for AS%i', asn)
            return set()
        for poc in data['poc_set']:
            all_emails.add(poc['email'])
            if role.lower() in poc['role'].lower() or role.lower() in poc['name'].lower():
                contact_emails.add(poc['email'])
                found = True
        return contact_emails if found else all_emails

    def prepare_email(self, asn_l: int, asn_r: int) -> tuple[set, str, str]:
        """Returns everything that will be in the email if any"""
        sessions = self.format_sessions(asn_l, asn_r)
        grouped_sessions: dict = {}
        peering_matrix = self.peering_matrix(asn_l, asn_r)
        for session in sessions:
            grouped_sessions.setdefault(session['status'], []).append(session)

        if not grouped_sessions or list(grouped_sessions.keys()) == ['Established']:
            logger.info('Nothing to do')
            return set(), '', ''
        if 'Idle' in grouped_sessions:
            logger.info('Please clear the idle BGP session(s) using the "clear" cookbook action.')
            return set(), '', ''
        them_human = f"{sessions[0]['name_r']} (AS{asn_r})"
        body = f"Hello {them_human},\n\n"

        subject_actions = []
        established = False
        recipients = set()
        if 'Established' in grouped_sessions:
            established = True
            body += f"We currently have {len(grouped_sessions['Established'])} established session(s) "\
                    "between our networks and we thank you for that.\n\n"

        # New peer, no existing sessions
        if 'Not configured' in grouped_sessions:
            recipients.update(self.contact_emails(asn_r, 'peering'))
            if established:
                subject_actions.append('additional peering opportunity')
                body += "According to PeeringDB, it looks like we can extend "\
                        "our peering relationship to new IXP(s).\n"
            else:
                subject_actions.append('peering opportunity')
                body += "According to PeeringDB, both our networks are present at the same IXP(s).\n"\

            body += "We would like to setup those sessions in order to improve the latency and reliability "\
                    "between our networks.\n"\
                    "If you agree please have a look at the table below, "\
                    "configure your side of the sessions and let us know.\n"\
                    "A prefix limit of 10 and no MD5 is preferred.\n\n"

        if 'Active' in grouped_sessions:
            recipients.update(self.contact_emails(asn_r, 'noc'))
            subject_actions.append('DOWN session(s)')
            body += """
According to our monitoring one or more BGP session(s) between our networks is in a DOWN state.
Please see the table below and let us know.
As we use data from PeeringDB please make sure your records are up to date.
If no reply or an extended downtime, we will have to delete the session(s).\n\n"""

        if not subject_actions:
            raise RuntimeError("Not all sessions are established, but can't automatically find what's wrong.")

        subject = f"{' and '.join(subject_actions)} between the Wikimedia Foundation (AS14907) and {them_human}"
        body += str(peering_matrix) + '\n' + FOOTER

        return recipients, subject, body

    def send_email(self, recipients, subject, body) -> None:
        """Actually send the email"""
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = 'peering@wikimedia.org'
        msg['To'] = ', '.join(recipients)
        msg['Cc'] = 'peering@wikimedia.org'
        msg['Auto-Submitted'] = 'auto-generated'
        msg.set_content(body)

        with smtplib.SMTP('localhost') as smtp:
            smtp.send_message(msg)
            logger.info('📧 Email sent')

    def email(self, asn_l: int, asn_r: int) -> None:
        """Prepare the email, display it and send it"""
        recipients, subject, body = self.prepare_email(asn_l, asn_r)
        if not subject:
            return
        print(f'\nSubject: {subject} \n\n {body}')
        if not recipients:
            logger.error('No recipients')
            return
        if self.dry_run:
            logger.info('Dry-run: not sending the email to %s.', str(recipients))
        else:
            ask_confirmation(f'Send the above email to {str(recipients)} ?')
            self.send_email(recipients, subject, body)
