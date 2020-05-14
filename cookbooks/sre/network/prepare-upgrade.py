"""Prepare the software upgrade of a network device

- Junos only
- Cleanup storage
- Download image from apt server
- Compare checksums
- Verify image when able
- Save rescue config

Usage example:
    cookbook sre.network.prepare-upgrade junos-vmhost-install-mx-x86-64-18.2R3-S3.11.tgz cr3-ulsfo.wikimedia.org

"""
import argparse
import json
import logging

from spicerack.interactive import ensure_shell_is_durable

__title__ = 'Prepare the software upgrade of a network device'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


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
        # Only return output if there is an issue
        logger.info(lines)
    return False


def output_to_json(results):
    """Parse a json output."""
    for _, output in results:
        result = output.message().decode()
        try:
            result_json = json.loads(result)
        except json.decoder.JSONDecodeError as e:
            logger.error(result)
            logger.error('Can\'t parse output as json.')
            logger.error(e)
            return False
        return result_json


def run(args, spicerack):
    """Required by Spicerack API."""
    ensure_shell_is_durable()

    logger.info('Get source image checksum')
    image_server = spicerack.dns().resolve_cname('apt.wikimedia.org')
    remote = spicerack.remote()
    image_server = remote.query(image_server)
    cmd = "sha1sum /srv/junos/{} | cut -d' ' -f1".format(args.image)
    results = image_server.run_sync(cmd)
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

    logger.info('Cleanup device storage')
    results = device.run_sync('request system storage cleanup no-confirm | display json')
    json_output = output_to_json(results)
    if not json_output:
        return 1
    if 'success' not in json_output['system-storage-cleanup-information'][0]:
        logger.info(json_output)
        logger.error('Command did not run successfully')
        return 1

    logger.info('Copy image to device')
    cmd = 'file copy "https://apt.wikimedia.org/junos/{}" /var/tmp/'.format(args.image)
    device.run_sync(cmd)

    logger.info('Compare checksums')
    cmd = 'file checksum sha1 /var/tmp/{} | display json'.format(args.image)

    results = device.run_sync(cmd)
    json_output = output_to_json(results)
    if not json_output:
        return 1
    try:
        dst_checksum = json_output['checksum-information']['file-checksum'][0]['checksum'][0]['data']
    except KeyError:
        logger.info(json_output)
        logger.error('Can\'t generate destination side checksum, did the file copy go well?')
        return 1

    if src_checksum != dst_checksum:
        logger.error('Checksum missmatch, maybe partial file transfer?')
        return 1

    logger.info('Save rescue config')
    results = device.run_sync('request system configuration rescue save | display json')
    json_output = output_to_json(results)
    if not json_output:
        return 1
    if 'success' not in json_output['rescue-management-results'][0]['routing-engine'][0]:
        logger.info(json_output)
        logger.error('Command did not run successfully.')
        return 1

    logger.info('Validate image')
    if 'vmhost' in args.image:
        logger.info('Introduced in Junos OS Release 18.4R1, good luck.')
    else:
        cmd = 'request system software validate /var/tmp/{}.tgz'.format(args.image)
        if not present_in_output(device.run_sync(cmd), 'Validation succeeded'):
            logger.error('Validation failed, try running it manually.')
            return 1
    logger.info('Ready for next cookbook')
    return 0
