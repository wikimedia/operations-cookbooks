"""Manage CF BGP advertisement of our prefixes.

- Per prefix
- TODO: add site slug to CF description field

Usage example:
    cookbook sre.network.cf status all
    cookbook sre.network.cf start X.X.X.X/Y
    cookbook sre.network.cf stop X.X.X.X/Y
    cookbook sre.network.cf stop all

"""
import argparse
import getpass
import logging
import requests


__title__ = 'Manage CF BGP advertisement of our prefixes'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name
CF_AUTH_EMAIL = 'noc@wikimedia.org'
CF_BASE_URL = 'https://api.cloudflare.com/client/v4/accounts/657bdb51dfa3942bbab412ce5ab7bb73'


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('action', choices=['start', 'stop', 'status'])
    parser.add_argument('query', help="Prefix in the X.X.X.X/Y form or 'all'")
    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    # TODO: Move the token to puppet-private so we don't have to prompt for it
    api_token = getpass.getpass("Insert NOC API token (it's in pwstore):")
    if not api_token:
        logger.error("Can't be empty 👎")
        return 1

    headers = {'Content-Type': 'application/json', 'X-Auth-Key': api_token, 'X-Auth-Email': CF_AUTH_EMAIL}

    # Get all the configured prefixes
    response = requests.get('{base_url}/addressing/prefixes'.format(base_url=CF_BASE_URL),
                            headers=headers, proxies=spicerack.requests_proxies)
    list_prefixes = parse_cf_response('list all prefixes', response)

    # Store the update action result, so we don't interrupt the run if there is an error
    # But still return a cookbook failure if at least one update fails.
    return_code = 0

    # Iterate over all prefixes
    for prefix in list_prefixes['result']:
        # Only care about the ones we select (or all)
        if prefix['cidr'] != args.query and prefix['description'] != args.query and args.query != 'all':
            logger.debug('Skipping prefix %(cidr)s, not matching query', prefix)
            continue

        log_prefix_status(prefix)
        if args.action == 'status':
            continue

        advertise = args.action == 'start'
        if prefix['advertised'] == advertise:
            logger.info('Prefix %(cidr)s already in desired state', prefix)
            continue

        if spicerack.dry_run:
            logger.debug('Skipping update of prefix %(cidr)s in DRY-RUN mode', prefix)
            continue

        try:
            update_prefix_status(headers, prefix, advertise, spicerack.requests_proxies)
        except Exception as e:  # Don't interrupt the run if there is an error
            logger.error('⚠️  Failed to update prefix {cidr}: {e}'.format(cidr=prefix['cidr'], e=e))
            return_code = 1

    return return_code


def update_prefix_status(headers, prefix, advertise, proxies):
    """Update the prefix's status

    Arguments:
        headers (dict): HTTP headers.
        prefix (dict): the prefix dictionary.
        advertise (bool): action to perform on prefix.
        proxies (dict): proxies setting for Python Requests calls.

    Raises:
        Exception: on error.

    """
    data = {'advertised': advertise}
    url = '{base_url}/addressing/prefixes/{prefix_id}/bgp/status'.format(base_url=CF_BASE_URL, prefix_id=prefix['id'])
    action = 'update prefix {cidr}'.format(cidr=prefix['cidr'])

    response = requests.patch(url, headers=headers, json=data, proxies=proxies)

    update_prefix = parse_cf_response(action, response)
    if update_prefix['result']['advertised'] == advertise:
        logger.info('👍 Update successful')
        prefix.update(update_prefix['result'])
        log_prefix_status(prefix)
        return

    raise RuntimeError('Unexpected response: {err}'.format(err=update_prefix))


def parse_cf_response(action, response):
    """Parse a response from the CF API and raise exception on error.

    Arguments:
        action (str): the action performed for logging purposes.
        response (requests.models.Response): the response to parse.

    Returns:
        dict: the parsed JSON from the response.

    Raises:
        RuntimeError: on validation failure.
        json.decoder.JSONDecodeError: on invalid JSON.

    """
    err_format = '⚠️ Failed to {action} (HTTP={code}):\n{err}'
    if response.status_code != 200:
        raise RuntimeError(err_format.format(action=action, code=response.status_code, err=response.text))

    result = response.json()
    if not result['success']:
        raise RuntimeError(err_format.format(action=action, code=response.status_code, err=result))

    for message in result['messages']:
        logger.info(message)

    return result


def log_prefix_status(prefix):
    """Log the status of a prefix.

    Arguments:
        prefix (dict): a prefix dictionary as returned by CF API.

    """
    params = prefix.copy()
    params['is_advertised'] = '' if prefix['advertised'] else 'not '
    logger.info('Prefix %(cidr)s: %(is_advertised)sadvertised since %(advertised_modified_at)s, '
                'on_demand=%(on_demand_enabled)s', params)
