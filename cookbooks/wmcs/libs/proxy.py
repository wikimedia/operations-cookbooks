#!/usr/bin/env python3
"""Functions to setup a socks proxy"""
from __future__ import annotations

import base64
import logging
import os
import subprocess
from contextlib import contextmanager
from pathlib import Path

import requests
from spicerack import Spicerack
from spicerack.alertmanager import ALERTMANAGER_URLS
from wmflib.config import load_yaml_config

BASE64_PUPPET_CA_URL = (
    "https://gerrit.wikimedia.org/r/plugins/gitiles/operations/puppet/"
    "+/refs/heads/production"
    "/modules/profile/files/puppet/ca.production.pem"
    "?format=TEXT"
)
LOGGER = logging.getLogger(__name__)


def _is_proxy_working() -> bool:
    try:
        requests.get(
            ALERTMANAGER_URLS[0],
            proxies=dict(
                http="socks5h://127.0.0.1:8888",
                https="socks5h://127.0.0.1:8888",
            ),
            timeout=5,
        )
    except (requests.ConnectTimeout, requests.ConnectionError):
        return False

    if os.environ.get("http_proxy") == os.environ.get("https_proxy") == "socks5h://127.0.0.1:8888":
        return True

    return False


def _start_proxy(puppet_ca_path: Path, port: int) -> None:
    if _is_proxy_working():
        _stop_proxy(port=port)

    if "http_proxy" in os.environ:
        del os.environ["http_proxy"]
    if "https_proxy" in os.environ:
        del os.environ["https_proxy"]
    if "REQUESTS_CA_BUNDLE" in os.environ:
        del os.environ["REQUESTS_CA_BUNDLE"]

    subprocess.run(
        [
            "/usr/bin/ssh",
            # Do not run any command
            "-N",
            # Drop to the background
            "-f",
            # Start a socks proxy
            "-D",
            f"127.0.0.1:{port}",
            "cumin1001.eqiad.wmnet",
        ],
        check=True,
    )
    os.environ["http_proxy"] = f"socks5h://127.0.0.1:{port}"
    os.environ["https_proxy"] = f"socks5h://127.0.0.1:{port}"
    os.environ["REQUESTS_CA_BUNDLE"] = str(puppet_ca_path.resolve().absolute())


def _stop_proxy(port: int) -> None:
    subprocess.run(["/usr/bin/pkill", "-f", f"D 127.0.0.1:{port}.*cumin1001.eqiad.wmnet"], check=True)
    if "http_proxy" in os.environ:
        del os.environ["http_proxy"]
    if "https_proxy" in os.environ:
        del os.environ["https_proxy"]
    if "REQUESTS_CA_BUNDLE" in os.environ:
        del os.environ["REQUESTS_CA_BUNDLE"]


def _download_puppet_ca(puppet_ca_path: Path):
    if not puppet_ca_path.exists():
        response = requests.get(BASE64_PUPPET_CA_URL, timeout=10)
        response.raise_for_status()
        raw_puppet_ca = base64.b64decode(response.text)
        puppet_ca_path.write_bytes(raw_puppet_ca)


@contextmanager
def with_proxy(spicerack: Spicerack):
    """Context manager that makes sure to start and tear down a socks proxy if needed.

    Used to be able to access internal apis when running from your laptop/remotely.
    """
    config = load_yaml_config(config_file=spicerack.config_dir / "wmcs.yaml", raises=False)
    LOGGER.info("Loading socks proxy config from %s", spicerack.config_dir / "wmcs.yaml")
    socks_proxy_port = int(config.get("socks_proxy_port", "54123"))
    puppet_ca_path = (
        Path(config.get("puppet_ca_path", spicerack.config_dir / "puppet_ca.crt")).expanduser().resolve().absolute()
    )
    proxy_started = False
    if not _is_proxy_working():
        try:
            LOGGER.info("Starting socks proxy on 127.0.0.1:%d", socks_proxy_port)
            _download_puppet_ca(puppet_ca_path=puppet_ca_path)
            _start_proxy(port=socks_proxy_port, puppet_ca_path=puppet_ca_path)
            proxy_started = True
        except Exception as error:  # pylint: disable=broad-except
            LOGGER.warning(
                "Unable to start the socks proxy, trying to run the cookbook without it... exception:%s", str(error)
            )
    else:
        LOGGER.info(
            "Proxy already running."
            if os.environ.get("https_proxy", None) is not None
            else "We already have access without proxy, skipping..."
        )
    try:
        yield
    finally:
        if proxy_started:
            LOGGER.info("Stopping proxy on 127.0.0.1:%d", socks_proxy_port)
            _stop_proxy(port=socks_proxy_port)
        else:
            LOGGER.info("The proxy was not started, not stopping.")
