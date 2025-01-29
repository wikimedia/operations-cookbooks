"""Network Cookbooks"""
import json
import logging

from ipaddress import ip_address
from typing import Optional

from spicerack.netbox import Netbox
from spicerack.remote import Remote, RemoteHosts
from wmflib.interactive import ask_confirmation

__title__ = __doc__
__owner_team__ = "Infrastructure Foundations"
logger = logging.getLogger(__name__)


def configure_switch_interfaces(remote: Remote, netbox: Netbox, netbox_data: dict,
                                print_output: bool = False) -> None:
    """Configure the switch interfaces relevant to a device.

    Arguments:
        remote: Spicerack remote instance
        netbox: pynetbox instance on the selected Netbox server
        netbox_data: Dict of netbox fields about the target device
        print_output: Display a more verbose output

    """
    # Get all the device's main interfaces (production, connected)
    # Most servers have 1 uplink, some have 2, a few have more
    nb_device_interfaces = list(netbox.api.dcim.interfaces.filter(
        device_id=netbox_data['id'], mgmt_only=False, connected=True))
    if not nb_device_interfaces:
        raise RuntimeError(f"No non-mgmt connected interfaces found for {netbox_data['name']}. Please check Netbox.")
    # Tackle interfaces one at a time
    for nb_device_interface in nb_device_interfaces:

        nb_switch_interface = nb_device_interface.connected_endpoints[0]
        # Get the switch FQDN (VC or not VC)
        vc = nb_switch_interface.device.virtual_chassis
        if vc:
            switch_fqdn = vc.name
        else:
            switch_fqdn = nb_switch_interface.device.primary_ip.dns_name
        logger.debug("%s is connected to %s:%s", nb_device_interface, switch_fqdn, nb_switch_interface)

        remote_host = remote.query('D{' + switch_fqdn + '}')
        # Get the live interface config adds them in alphabetic order in a Netbox like dict,
        # or None if it doesn't exist at all
        live_interface = get_junos_live_interface_config(remote_host, nb_switch_interface.name, print_output)

        commands = junos_set_interface_config(netbox_data, live_interface, nb_switch_interface)

        if commands:
            run_junos_commands(remote_host, commands)
        else:
            logger.info("No configuration change needed on the switch for %s", nb_device_interface)


def junos_set_interface_config(netbox_data: dict, live_interface: Optional[dict],  # pylint: disable=too-many-branches
                               nb_switch_interface) -> list[str]:
    """Return a list of Junos set commands needed to configure the interface

    Arguments:
        netbox_data: Dict of netbox fields about the target device
        live_interface: running configuration of a given Junos interface in a Netbox format
        nb_switch_interface: Instance of relevant Netbox interface

    """
    commands: list[str] = []
    device_name = netbox_data['name']
    switch_model = nb_switch_interface.device.device_type.slug
    # We want to disable the interface if it's disabled in Netbox
    if not nb_switch_interface.enabled:
        # If there is already something configured and the interface is enabled: clear it
        if live_interface and live_interface['enabled']:
            # But first a safeguard
            if device_name not in live_interface['description']:
                logger.error("Need to disable %s:%s, "
                             "but the switch interface description doesn't match the server name:\n"
                             "%s vs %s",
                             nb_switch_interface.device,
                             nb_switch_interface,
                             device_name,
                             live_interface['description'])
                return commands
            # Delete the interface to clear all its properties
            commands.append(f"delete interfaces {nb_switch_interface}")

        commands.extend([f'set interfaces {nb_switch_interface} description "DISABLED"',
                         f"set interfaces {nb_switch_interface} disable",
                         ])
        if not nb_switch_interface.device.name.startswith("fasw"):
            commands.append(f"delete class-of-service interfaces {nb_switch_interface}")
        if switch_model.startswith('qfx5120'):
            commands.append(f"delete protocols sflow interfaces {nb_switch_interface}.0")
    else:  # the interface is enabled in Netbox
        # The interface doesn't exists yet on the device, configure it
        # Status
        if live_interface and not live_interface['enabled']:
            commands.append(f'delete interfaces {nb_switch_interface} disable')
        # Description
        description = device_name
        # Safeguard for accidental " that would be interpreted as an end of comment by Junos
        cable_label = nb_switch_interface.cable.label
        if cable_label and '"' not in cable_label:
            description += f" {{#{cable_label}}}"
        if not live_interface or live_interface['description'] != description:
            commands.append(f'set interfaces {nb_switch_interface} description "{description}"')

        # MTU
        if nb_switch_interface.mtu and (not live_interface or live_interface['mtu'] != nb_switch_interface.mtu):
            commands.append(f'set interfaces {nb_switch_interface} mtu {nb_switch_interface.mtu}')

        # VLAN
        if nb_switch_interface.mode:
            # Interface mode
            if not live_interface or live_interface['mode'] != nb_switch_interface.mode.value:
                # Junos call it trunk, Netbox tagged
                junos_mode = 'access' if nb_switch_interface.mode.value == 'access' else 'trunk'
                commands.append(f'set interfaces {nb_switch_interface} unit 0 family '
                                f'ethernet-switching interface-mode {junos_mode}')

            vlans_members = []
            # Native vlan
            if nb_switch_interface.mode.value == 'tagged' and nb_switch_interface.untagged_vlan:
                if (not live_interface or
                   live_interface['native-vlan-id'] != nb_switch_interface.untagged_vlan.vid):
                    commands.append(f'set interfaces {nb_switch_interface} '
                                    f'native-vlan-id {nb_switch_interface.untagged_vlan.vid}')
            if nb_switch_interface.untagged_vlan:
                vlans_members.append(nb_switch_interface.untagged_vlan.name)
            for tagged_vlan in nb_switch_interface.tagged_vlans or []:
                vlans_members.append(tagged_vlan.name)
            if not live_interface or live_interface['vlans'] != sorted(vlans_members):
                if len(vlans_members) == 0:
                    logger.error("No vlans configured for %s:%s",
                                 nb_switch_interface.device,
                                 nb_switch_interface)
                else:
                    # Delete the configured vlans, and re-add the needed ones
                    commands.append(f'delete interfaces {nb_switch_interface} unit 0 family '
                                    f'ethernet-switching vlan members')
                    commands.append(f'set interfaces {nb_switch_interface} unit 0 family '
                                    f"ethernet-switching vlan members [ {' '.join(sorted(vlans_members))} ]")

        # QoS & Sflow config for the interface - not currently done on fasw
        if nb_switch_interface.device.name.startswith("fasw"):
            return commands
        commands.append(f"delete class-of-service interfaces {nb_switch_interface}")
        commands.append(f"set class-of-service interfaces {nb_switch_interface} unit 0 "
                        "classifiers dscp v4_classifier")
        if switch_model.startswith('qfx5120'):
            commands.append(f"set class-of-service interfaces {nb_switch_interface} scheduler-map wmf_map")
            commands.append(f"set class-of-service interfaces {nb_switch_interface} unit 0 "
                            "classifiers dscp-ipv6 v6_classifier")
            commands.append(f"set protocols sflow interfaces {nb_switch_interface}.0")
        elif switch_model.startswith(('qfx5100', 'ex4')):
            commands.append(f"set class-of-service interfaces {nb_switch_interface} forwarding-class-set "
                            "wmf_classes output-traffic-control-profile wmf_tc_profile")

    return commands


def run_junos_commands(remote_host: RemoteHosts, conf_commands: list) -> None:
    """Run commands on Juniper devices, first load the commands show the diff then exit.

       Then commit confirm it.
       Then commit check.

    Arguments:
        remote_host: Spicerack RemoteHosts instance
        conf_commands: list of Junos set commands to run

    """
    # Once we get more trust in the system, we could commit the change without prompting the user.
    for mode in ['compare', 'commit', 'confirm']:
        is_safe = False
        commands = ['configure exclusive']  # Enter configuration mode with a lock on the config
        commands.extend(conf_commands)  # Add the actions

        if mode == 'compare':
            commands.extend(['show|compare',  # Get a diff
                             'rollback',  # Leave no trace
                             'exit'])  # Cleanly close
            is_safe = True
        elif mode == 'commit':
            commands.extend(['show|compare',  # Get a diff
                             'commit confirmed 1',  # Auto rollback if any issues
                             'exit'])  # Cleanly close
        elif mode == 'confirm':
            commands = ['configure',
                        'commit check',
                        'exit']

        results_raw = remote_host.run_sync(';'.join(commands), is_safe=is_safe, print_progress_bars=False)
        if mode == 'compare':
            ask_confirmation('Commit the above change?')
        elif mode == 'commit':
            output_lines = RemoteHosts.results_to_list(results_raw)[0][1].split('\n')
            if output_lines[-2] != 'commit complete':
                raise RuntimeError('JunOS config commit failed - see above - device may need Homer run')
            logger.info('Commited the above change, needs to be confirmed')
        elif mode == 'confirm':
            logger.info('Change confirmed')


def parse_results(results_raw, json_output=False):
    """Parse a single device cumin output."""
    # Only supports 1 target device at a time
    results = RemoteHosts.results_to_list(results_raw)
    if not results:  # In dry run, run_sync/async will return an empty dict
        return None
    result = results[0][1]
    # If empty result (eg. interface not configured)
    if not result:
        return None
    if json_output:
        return json.loads(result)
    return result


def junos_interface_to_netbox(config: dict, old_junos: bool) -> dict:
    """Converts a Junos JSON interface config to a dict similar to Netbox interfaces.

    Arguments:
        config: interface json config
        old_junos: if the JSON returned is ancient

    """
    interface = {
        'name': config['name']['data'] if old_junos else config['name'],
        'enabled': 'disable' not in config,
        'description': config.get('description', [{}])[0].get('data') if old_junos else config.get('description'),
        'mode': None,
        'vlans': None
    }
    if old_junos:
        for key in ('mtu', 'native-vlan-id'):
            interface[key] = int(config.get(key, [{}])[0].get('data', 0)) or None
    else:
        for key in ('mtu', 'native-vlan-id'):
            interface[key] = config.get(key)
    # vlans
    try:
        if old_junos:
            eth_sw = config['unit'][0]['family'][0]['ethernet-switching'][0]
        else:
            eth_sw = config['unit'][0]['family']['ethernet-switching']
    except (IndexError, KeyError):
        logger.debug('No ethernet switching configured.')
        return interface

    if 'interface-mode' in eth_sw:
        # Junos call it access and trunk
        # Nebox use a dict with label (eg. Access) and value (eg. access), keeping it simpler here
        interface_mode = eth_sw['interface-mode'][0]['data'] if old_junos else eth_sw['interface-mode']
        interface['mode'] = 'access' if interface_mode == 'access' else 'tagged'

    # get a usable set of configured vlans
    vlans = []
    if 'vlan' in eth_sw:
        if old_junos:
            for vlan_raw in eth_sw['vlan'][0]['members']:
                vlans.append(vlan_raw['data'])
        else:
            vlans = eth_sw['vlan']['members']

    interface['vlans'] = sorted(vlans)

    return interface


def get_junos_live_interface_config(remote_host: RemoteHosts, interface: str,
                                    print_output: bool = False) -> Optional[dict]:
    """Returns the running configuration of a given Junos interface in a Netbox format.

    Arguments:
        remote_host: Spicerack RemoteHosts instance
        interface: target interface name
        print_output: Display a more verbose output

    """
    # Get the interface config
    logger.debug("Fetching the live interface config")
    results_raw = remote_host.run_sync(f"show configuration interfaces {interface} | display json",
                                       is_safe=True,
                                       print_output=print_output,
                                       print_progress_bars=False)
    try:
        result_json = parse_results(results_raw, json_output=True)
        if isinstance(result_json['configuration'], list):
            old_junos = True
            interface_json = result_json['configuration'][0]['interfaces'][0]['interface'][0]
        elif isinstance(result_json['configuration'], dict):
            old_junos = False
            interface_json = result_json['configuration']['interfaces']['interface'][0]
        else:
            logger.error('Network device returned unknown data: "%s"', result_json)
            return None
    except (KeyError, TypeError) as e:
        logger.error('Network device returned invalid data: "%s". Error: %s', results_raw, e)
        return None
    return junos_interface_to_netbox(interface_json, old_junos)


def get_junos_optics(remote_host: RemoteHosts, interface: str, print_output: bool = False) -> dict:
    """Returns the oper optical status for a given interface

    Arguments:
        remote_host: Spicerack RemoteHosts instance
        interface: target interface name
        print_output: Display a more verbose output

    Returns:
        Dict of relevant input/ouput optic levels/power or None

    """
    logger.debug("Fetching the interface optical status")
    results_raw = remote_host.run_sync(f"show interfaces diagnostics optics {interface} | display json",
                                       is_safe=True,
                                       print_output=print_output,
                                       print_progress_bars=False)

    result_json = parse_results(results_raw, json_output=True)
    if not result_json:
        logger.error("Interface %s either incorrect or not optical", interface)
        return {}
    optics_diag = result_json['interface-information'][0]['physical-interface'][0]['optics-diagnostics'][0]
    interface_optic = {}
    for key in ('laser-output-power',
                'laser-output-power-dbm',
                'rx-signal-avg-optical-power',
                'rx-signal-avg-optical-power-dbm'):
        try:
            interface_optic[key] = optics_diag.get(key)[0].get('data')
        except TypeError:
            logger.error("Key '%s' not present in returned data", key)
    if ('rx-signal-avg-optical-power-dbm' in interface_optic
       and float(interface_optic['rx-signal-avg-optical-power-dbm']) < -30):
        logger.error('RX power too low')
    return interface_optic


def get_junos_interface(remote_host: RemoteHosts, interface: str, print_output: bool = False) -> Optional[dict]:
    """Returns the interface status of a given interface

    Arguments:
        remote_host: Spicerack RemoteHosts instance
        interface: target interface name
        print_output: Display a more verbose output

    Returns:
        Dict of relevant interface metrics or None

    """
    logger.debug("Fetching the interface status")
    results_raw = remote_host.run_sync(f"show interfaces {interface} extensive | display json",
                                       is_safe=True,
                                       print_output=print_output,
                                       print_progress_bars=False)

    result_json = parse_results(results_raw, json_output=True)
    if not result_json:
        logger.error("Interface %s not found on the device", interface)
        return None
    physical_interface = result_json['interface-information'][0]['physical-interface'][0]
    interface_status = {}
    for key in ('admin-status',
                'oper-status',
                'interface-flapped'):
        try:
            interface_status[key] = physical_interface.get(key)[0].get('data')
        except TypeError:
            logger.error("Key '%s' not present in returned data", key)

    interface_status['errors'] = {}
    for list_type in ('input-error-list', 'output-error-list'):
        input_error_list = physical_interface[list_type][0]
        for key in input_error_list.keys():
            value = int(input_error_list.get(key, [{}])[0].get('data', 0))
            if value > 0:
                interface_status['errors'][key] = value

    logger.debug("Clear the interface statistics")
    remote_host.run_sync(f"clear interfaces statistics {interface}",
                         print_output=print_output,
                         print_progress_bars=False)

    return interface_status


def get_junos_logs(remote_host: RemoteHosts, match: str, print_output: bool = False, last: int = 10) -> str:
    """Returns system logs

    Arguments:
        remote_host: Spicerack RemoteHosts instance
        match: pattern to find in the logs (grep)
        print_output: Display a more verbose output
        last: limit the output to the last X lines matching

    Returns:
        Log messages or None

    """
    logger.debug("Fetching log lines containing %s", match)
    # TODO catch invalid characters
    results_raw = remote_host.run_sync((f'show log messages | match "{match}" |'
                                        f' except UI_CMDLINE_READ_LINE | last {last}'),
                                       is_safe=True,
                                       print_output=print_output,
                                       print_progress_bars=False)

    result = parse_results(results_raw)
    if not result:
        logger.debug("No logs matching %s", match)
    return result


def get_junos_bgp_peer(remote_host: RemoteHosts, peer_ip: str, print_output: bool = False) -> dict:
    """Returns informations about a BGP peer

    Arguments:
        remote_host: Spicerack RemoteHosts instance
        peer_ip: target peer IP (v4 or v6)
        print_output: Display a more verbose output

    Returns:
        Dict of relevant peer data

    """
    # TODO: replace with SNMP or LibreNMS API to speed up and get time since last up?
    logger.debug("Fetching BGP status for peer %s", peer_ip)
    results_raw = remote_host.run_sync(f"show bgp neighbor {peer_ip} | display json",
                                       is_safe=True,
                                       print_output=print_output,
                                       print_progress_bars=False)

    result_json = parse_results(results_raw, json_output=True)
    # Even if the peer is not configured some json is returned
    if not result_json:
        logger.error("Problem while trying to get data for BGP peer %s", peer_ip)
        return {'status': 'Error fetching status'}

    if 'bgp-peer' not in result_json['bgp-information'][0]:
        return {'status': 'Not configured'}
    bgp_peer = result_json['bgp-information'][0]['bgp-peer'][0]
    peer_state = bgp_peer['peer-state'][0]['data']
    return {'status': peer_state}


def get_junos_bgp_summary(remote_host: RemoteHosts, print_output: bool = False) -> dict:
    """Returns summary of all BGP session on device

    Arguments:
        remote_host: Spicerack RemoteHosts instance
        print_output: Display a more verbose output

    Returns:
        Dict of relevant BGP summary data

    """
    # TODO: replace with SNMP or LibreNMS API to speed up and get time since last up?
    logger.debug("Fetching BGP summary")
    results_raw = remote_host.run_sync("show bgp summary | display json",
                                       is_safe=True,
                                       print_output=print_output,
                                       print_progress_bars=False)

    result_json = parse_results(results_raw, json_output=True)
    # Even if the peer is not configured some json is returned
    if not result_json or 'bgp-peer' not in result_json['bgp-information'][0]:
        # This should never happen as all routers have BGP
        # Raising an error to make sure cookbooks don't think
        # it's just a missing session
        raise RuntimeError("Router didn't return any valid BGP data")

    bgp_peers = result_json['bgp-information'][0]['bgp-peer']

    formatted_peers = {}

    for bgp_peer in bgp_peers:
        peer_address = ip_address(bgp_peer['peer-address'][0]['data'])
        peer_as = int(bgp_peer['peer-as'][0]['data'])
        formatted_peers[peer_address] = {'peer-as': peer_as}
        # TODO add received/accepted prefix count
        for key in ('elapsed-time',
                    'description',
                    'peer-state'):
            try:
                formatted_peers[peer_address][key] = bgp_peer.get(key)[0].get('data')
            except TypeError:
                logger.error("Key '%s' not present in returned data for %s", key, peer_address)

    return formatted_peers
