"""WDQS Cookbooks"""
__title__ = __doc__


def check_host_is_wdqs(remote_hosts, remote):
    """Remote hosts must be a wdqs host"""
    all_wdqs = remote.query("A:wdqs-all")
    if remote_hosts.hosts not in all_wdqs.hosts:
        raise ValueError("Selected hosts ({hosts}) must be WDQS hosts ".format(hosts=remote_hosts.hosts))
