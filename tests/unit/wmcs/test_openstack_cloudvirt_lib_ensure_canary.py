from cookbooks.wmcs.openstack.cloudvirt.lib.ensure_canary import FLAVOR, IMAGE, calculate_changelist

HYPERVISORS = [
    "cloudvirt4001",
    "cloudvirt4002",
    "cloudvirt4003",
]

CORRECT_VM_LIST = [
    {
        "Name": "canary4001-1",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4002-1",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4003-1",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
]


def test_no_changes():
    changelist = calculate_changelist(HYPERVISORS, CORRECT_VM_LIST, recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert not hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 0
        assert not hostchanges.needs_create
        assert not hostchanges.to_force_reboot


def test_recreate():
    changelist = calculate_changelist(HYPERVISORS, CORRECT_VM_LIST, recreate=True)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


def test_create_with_recreate():
    changelist = calculate_changelist(HYPERVISORS, [], recreate=True)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 0
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


def test_create_without_recreate():
    changelist = calculate_changelist(HYPERVISORS, [], recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 0
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


MISPLACED_VM_LIST = [
    {
        "Name": "canary4001-1",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4002-1",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4003-1",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
]


def test_misplaced_no_recreate():
    changelist = calculate_changelist(HYPERVISORS, MISPLACED_VM_LIST, recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert not hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


def test_misplaced_recreate():
    changelist = calculate_changelist(HYPERVISORS, MISPLACED_VM_LIST, recreate=True)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert not hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


ERROR_VM_LIST = [
    {
        "Name": "canary4001-1",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4002-1",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4003-1",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
]


def test_error_vm_no_recreate():
    changelist = calculate_changelist(HYPERVISORS, ERROR_VM_LIST, recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 0
        assert not hostchanges.needs_create
        assert hostchanges.to_force_reboot


def test_error_vm_recreate():
    changelist = calculate_changelist(HYPERVISORS, ERROR_VM_LIST, recreate=True)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


MULTIPLE_ERROR_VM_LIST = [
    {
        "Name": "canary4001-1",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4001-2",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4002-1",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4002-2",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4003-1",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4003-2",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
]


def test_multiple_error_vm():
    changelist = calculate_changelist(HYPERVISORS, MULTIPLE_ERROR_VM_LIST, recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert not hostchanges.needs_create
        assert hostchanges.to_force_reboot.startswith(hostchanges.vm_prefix)


def test_multiple_error_vm_recreate():
    changelist = calculate_changelist(HYPERVISORS, MULTIPLE_ERROR_VM_LIST, recreate=True)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 2
        assert hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.to_delete[1].startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


ACTIVE_AND_ERROR_VM_LIST = [
    {
        "Name": "canary4001-1",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4001-2",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4001-3",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4002-1",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4002-2",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4002-2",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4003-1",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4003-2",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4003-3",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
]


def test_active_and_error_vm_no_recreate():
    changelist = calculate_changelist(HYPERVISORS, ACTIVE_AND_ERROR_VM_LIST, recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 2
        assert hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.to_delete[1].startswith(hostchanges.vm_prefix)
        assert not hostchanges.needs_create
        assert not hostchanges.to_force_reboot


def test_active_and_error_vm_recreate():
    changelist = calculate_changelist(HYPERVISORS, ACTIVE_AND_ERROR_VM_LIST, recreate=True)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 3
        assert hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.to_delete[1].startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


WRONG_FLAVOR_VM_LIST = [
    {
        "Name": "canary4001-1",
        "Host": "cloudvirt4001",
        "Flavor Name": "whatever",
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4002-1",
        "Host": "cloudvirt4002",
        "Flavor Name": "whatever",
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
    {
        "Name": "canary4003-1",
        "Host": "cloudvirt4003",
        "Flavor Name": "whatever",
        "Image Name": IMAGE,
        "Status": "ERROR",
    },
]


def test_wrong_flavor():
    changelist = calculate_changelist(HYPERVISORS, WRONG_FLAVOR_VM_LIST, recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


def test_wrong_flavor_recreate():
    changelist = calculate_changelist(HYPERVISORS, WRONG_FLAVOR_VM_LIST, recreate=True)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


WRONG_IMAGE_VM_LIST = [
    {
        "Name": "canary4001-1",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": "wrong",
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4002-1",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": "wrong",
        "Status": "ERROR",
    },
    {
        "Name": "canary4003-1",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": "wrong",
        "Status": "ERROR",
    },
]


def test_wrong_image():
    changelist = calculate_changelist(HYPERVISORS, WRONG_IMAGE_VM_LIST, recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


def test_wrong_image_recreate():
    changelist = calculate_changelist(HYPERVISORS, WRONG_IMAGE_VM_LIST, recreate=True)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert hostchanges.to_delete[0].startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


MANY_VM_LIST = [
    {
        "Name": "canary4001-1",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4001-2",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4001-3",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4002-1",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4002-2",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4002-3",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4003-1",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4003-2",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
    {
        "Name": "canary4003-3",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "ACTIVE",
    },
]


def test_delete_many():
    changelist = calculate_changelist(HYPERVISORS, MANY_VM_LIST, recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 2
        for vm in hostchanges.to_delete:
            assert vm.startswith(hostchanges.vm_prefix)
        assert not hostchanges.needs_create
        assert not hostchanges.to_force_reboot


def test_delete_many_recreate():
    changelist = calculate_changelist(HYPERVISORS, MANY_VM_LIST, recreate=True)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 3
        for vm in hostchanges.to_delete:
            assert vm.startswith(hostchanges.vm_prefix)
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot


WRONG_STATUS_VM_LIST = [
    {
        "Name": "canary4001-1",
        "Host": "cloudvirt4001",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "MIGRATING",
    },
    {
        "Name": "canary4002-1",
        "Host": "cloudvirt4002",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "RESIZING",
    },
    {
        "Name": "canary4003-1",
        "Host": "cloudvirt4003",
        "Flavor Name": FLAVOR,
        "Image Name": IMAGE,
        "Status": "SHUTDOWN",
    },
]


def test_wrong_status():
    changelist = calculate_changelist(HYPERVISORS, WRONG_STATUS_VM_LIST, recreate=False)
    assert len(changelist) == 3

    for hostchanges in changelist:
        assert hostchanges.has_changes()
        assert len(hostchanges.to_delete) == 1
        assert hostchanges.needs_create
        assert not hostchanges.to_force_reboot
