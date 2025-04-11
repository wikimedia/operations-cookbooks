"""Prepare the software upgrade of a network device

- Junos only
- Cleanup storage
- Download image from apt server
- Compare checksums
- Save rescue config

Usage example:
    cookbook -v sre.network.prepare-upgrade junos-vmhost-install-mx-x86-64-18.2R3-S3.11.tgz cr3-ulsfo.wikimedia.org

"""
import argparse
import logging

from wmflib.interactive import ensure_shell_is_durable

from cookbooks.sre.network import parse_results


logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('image', help='Image filename')
    parser.add_argument('fqdn', help='Target FQDN')
    return parser


def present_in_output(results, find):
    """Check if a string is present from a cumin run command."""
    for _, output in results:
        lines = output.message().decode()
        if find in lines:
            return True
        logger.error(lines)
    return False


def run(args, spicerack):  # pylint: disable=too-many-return-statements
    """Required by Spicerack API."""
    ensure_shell_is_durable()

    logger.info('Get source image checksum')
    dns = spicerack.dns()
    image_server = dns.resolve_ptr(dns.resolve_ipv4('apt.wikimedia.org')[0])[0]
    remote = spicerack.remote()
    image_server = remote.query(image_server)
    results = image_server.run_sync(f"sha1sum /srv/junos/{args.image} | cut -d' ' -f1",
                                    is_safe=True,
                                    print_output=spicerack.verbose,
                                    print_progress_bars=False)
    for _, output in results:
        src_checksum = output.message().decode()
        break
    if len(src_checksum) != 40:
        logger.info(src_checksum)
        logger.error('Can\'t checksum, is the file there and readable?')
        return 1
    device = remote.query('D{' + args.fqdn + '}')
    if len(device.hosts) > 1:
        logger.error('Only 1 target device please.')
        return 1

    logger.info('Cleanup device storage 🧹')
    results = device.run_sync('request system storage cleanup no-confirm | display json',
                              print_output=spicerack.verbose,
                              print_progress_bars=False)
    json_output = parse_results(results, json_output=True)
    if not json_output or 'success' not in json_output['system-storage-cleanup-information'][0]:
        logger.info(json_output)
        logger.error('Command did not run successfully.')
        return 1

    logger.info('Copy image to device (this takes time ⏳)')
    device.run_sync(f'file copy "https://apt.wikimedia.org/junos/{args.image}" /var/tmp/',
                    print_output=spicerack.verbose,
                    print_progress_bars=False)

    logger.info('Compare checksums')
    results = device.run_sync(f'file checksum sha1 /var/tmp/{args.image} | display json',
                              is_safe=True,
                              print_output=spicerack.verbose,
                              print_progress_bars=False)

    json_output = parse_results(results, json_output=True)
    if not json_output:
        return 1
    try:
        dst_checksum = json_output['checksum-information'][0]['file-checksum'][0]['checksum'][0]['data']
    except KeyError:
        logger.info(json_output)
        logger.error('Can\'t generate destination side checksum, did the file copy go well?')
        return 1

    if src_checksum != dst_checksum:
        logger.error('Checksum missmatch, maybe partial file transfer?')
        return 1

    logger.info('Save rescue config')
    results = device.run_sync('request system configuration rescue save | display json',
                              print_output=spicerack.verbose,
                              print_progress_bars=False)
    json_output = parse_results(results, json_output=True)
    if not json_output or 'success' not in json_output['rescue-management-results'][0]['routing-engine'][0]:
        logger.info(json_output)
        logger.error('Command did not run successfully.')
        return 1

    logger.info('Validate image')
    os_type = 'vmhost' if 'vmhost' in args.image else 'system'
    cmd = f'request {os_type} software validate /var/tmp/{args.image}.tgz'

    results = device.run_sync(cmd,
                              print_output=spicerack.verbose,
                              print_progress_bars=False)
    if not present_in_output(results, 'Validation succeeded'):
        logger.error('Validation failed.')
        return 1
    logger.info('All good 👍')
    return 0
