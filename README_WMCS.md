# Wikimedia Cloud Services cookbooks
## Installation
Using your preferred method, install spicerack, the following uses virtualenv and virtualenvwrapper.
From the top of this repository, create a new virtualenv, and install the cookbooks (pulls the dependencies):
```
dcaro@vulcanus$  mkvirtualenv cookbooks
dcaro@vulcanus$  python setup.py install
```

To configure the cookbooks, just run the config generation script from the top of the repo, and follow the instruction:
```
dcaro@vulcanus$ wmcs utils/generate_wmcs_config.sh
```

This will generate the configuration files needed to run the cookbooks directly from this repository.

Now from anywhere , you should be able to run the `cookbook` command (adding something like `-c
~/.config/spicerack/cookbook.yaml` if you did not create the `/etc/spicerack/config.yaml` link).

**NOTE**: make sure you are in the virtualenv we created (`workon cookbooks`).

```
dcaro@vulcanus$ cookbook -l wmcs
cookbooks
`-- wmcs
    |-- wmcs.ceph
    |   |-- wmcs.ceph.osd
    |   |   `-- wmcs.ceph.osd.bootstrap_and_add
    |   |-- wmcs.ceph.reboot_node
...
    |       |-- wmcs.toolforge.worker.depool_and_remove_node
    |       `-- wmcs.toolforge.worker.drain
    `-- wmcs.vps
        |-- wmcs.vps.create_instance_with_prefix
        |-- wmcs.vps.refresh_puppet_certs
        `-- wmcs.vps.remove_instance
```
