"""VRTS Cookbooks"""

import logging

from packaging import version
from spicerack.remote import RemoteHosts

__title__ = __doc__

logger = logging.getLogger(__name__)


def get_current_version(host: RemoteHosts) -> version.Version:
    """Get the currently running VRTS version"""
    logger.info("Get current VRTS version from /opt/ on active host")
    results = host.run_sync(
        "readlink /opt/otrs", is_safe=True, print_progress_bars=False
    )  # readlink /opt/otrs -> /opt/znuny-6.5.8
    for _, output in results:
        lines = output.message().decode()
        for line in lines.splitlines():
            current_version = line.split("-")[1]
            return version.parse(current_version)
    raise RuntimeError(f"Could not retrieve current version from #{host}")
