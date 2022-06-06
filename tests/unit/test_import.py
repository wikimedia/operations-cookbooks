"""Test the import of all the cookbooks."""
import importlib
import pathlib
import os

from pkgutil import iter_modules
from setuptools import find_packages
from unittest import mock

import pytest


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


@pytest.mark.parametrize('module_name', get_modules())
def test_import(module_name):
    """It should successfully import all defined cookbooks and their packages."""
    with mock.patch('builtins.open', mock.mock_open(read_data=MOCKED_SRE_SWITCHDC_SERVICES_YAML)):
        importlib.import_module(module_name)  # Will raise on failure
