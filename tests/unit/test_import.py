"""Test the import of all the cookbooks."""
import importlib
import inspect
import pathlib
import os

from pkgutil import iter_modules
from setuptools import find_packages
from unittest import mock

import pytest

from spicerack.cookbook import CookbookBase


MOCKED_SRE_SWITCHDC_SERVICES_YAML = """---
a-service:
  active_active: true
  rec:
    codfw: a-service.svc.codfw.wmnet
    eqiad: a-service.svc.eqiad.wmnet
b-service:
  active_active: false
  rec:
    codfw: b-service.svc.codfw.wmnet
    eqiad: b-service.svc.eqiad.wmnet
"""
# SRE teams only from Puppet's Wmflib::Team (modules/wmflib/types/team.pp)
VALID_OWNER_TEAM = (
    "unowned",
    "Collaboration Services",
    "Data Persistence",
    "Data Platform",
    "Fundraising Tech",
    "Infrastructure Foundations",
    "Machine Learning",
    "Observability",
    "ServiceOps",
    "Traffic",
    "WMCS",
)
VALID_OWNER_TEAM_STR = "\n".join(VALID_OWNER_TEAM)


def get_modules():
    """Collect all the cookbook packages and modules."""
    base_package = "cookbooks"
    base_path = pathlib.Path(os.getcwd()) / base_package
    modules = set()
    for package in find_packages(base_path):
        modules.add(f"{base_package}.{package}")
        package_path = base_path / package.replace(".", "/")
        for module_info in iter_modules([str(package_path)]):
            if not module_info.ispkg:
                modules.add(f"{base_package}.{package}.{module_info.name}")

    return modules


@pytest.mark.parametrize("module_name", get_modules())
def test_import(module_name):
    """It should successfully import all defined cookbooks and their packages."""
    # avoid messing up with prometheus_client opening /proc/self/stat on module import
    if module_name.startswith("cookbooks.sre.switchdc.services"):
        mocked_open = mock.mock_open(read_data=MOCKED_SRE_SWITCHDC_SERVICES_YAML)
    else:
        mocked_open = open

    with mock.patch("builtins.open", mocked_open):
        importlib.import_module(module_name)  # Will raise on failure


@pytest.mark.parametrize("module_name", get_modules())
def test_owner_team(module_name):
    """If set the owner_team should have one of the valid values."""
    # avoid messing up with prometheus_client opening /proc/self/stat on module import
    if module_name.startswith("cookbooks.sre.switchdc.services"):
        mocked_open = mock.mock_open(read_data=MOCKED_SRE_SWITCHDC_SERVICES_YAML)
    else:
        mocked_open = open

    with mock.patch("builtins.open", mocked_open):
        module = importlib.import_module(module_name)
        for name, obj in inspect.getmembers(module):
            # Module API check
            if name == "__owner_team__":
                assert obj in VALID_OWNER_TEAM, (f"Module {module_name} has an invalid __owner_team__ '{obj}'. "
                                                 f"It must be one of:\n{VALID_OWNER_TEAM_STR}")

            # Class API check
            if inspect.isclass(obj) and issubclass(obj, CookbookBase) and obj is not CookbookBase:
                owner = getattr(obj, "owner_team", None)
                if owner is None:
                    continue

                assert owner in VALID_OWNER_TEAM, (f"Class {module_name}.{obj.__name__} has an invalid owner_team "
                                                   f"'{owner}'. Must be one of:\n{VALID_OWNER_TEAM_STR}")
