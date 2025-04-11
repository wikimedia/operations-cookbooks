"""Puppet Cookbooks"""

from typing import Union

from packaging import version
from requests import Session
from requests.exceptions import RequestException


__owner_team__ = "Infrastructure Foundations"


def get_puppet_fact(session: Session, host: str, fact: str) -> Union[str, None]:
    """Get a puppet fact from the puppetdb micro service for a specific host.

    Arguments:
        session: a request session used for fetching the facts
        host: the host to query
        fact: the fact to query

    """
    try:
        response = session.get(
            f"https://puppetdb-api.discovery.wmnet:8090/v1/facts/{fact}/{host}",
            timeout=10,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
    except RequestException as err:
        raise RuntimeError(f"Unable to get puppet version for: {host}") from err
    # the micro services returns a json string we just strip it instead of using json.loads
    return response.text.strip('"\n')


def get_puppet_version(session: Session, host: str) -> Union[version.Version, None]:
    """Get the puppet version for a specific host.

    Arguments:
        session: a request session used for fetching the facts
        host: the host to query
        fact: the fact to query

    """
    puppet_version = get_puppet_fact(session, host, 'puppetversion')
    if puppet_version is None:
        return None
    return version.parse(puppet_version)
