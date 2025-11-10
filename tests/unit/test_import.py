"""Global tests for all cookbooks."""
import argparse
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


@pytest.fixture(scope="class", params=get_modules())
def module_instance(request):
    """Load a given module and return it."""
    module_name = request.param
    # avoid messing up with prometheus_client opening /proc/self/stat on module import
    if module_name.startswith("cookbooks.sre.switchdc.services"):
        mocked_open = mock.mock_open(read_data=MOCKED_SRE_SWITCHDC_SERVICES_YAML)
    else:
        mocked_open = open

    with mock.patch("builtins.open", mocked_open):
        request.cls.module = importlib.import_module(module_name)


@pytest.mark.usefixtures("module_instance")
class TestCookbookModules:
    """Generic tests for each cookbook module. For each module the class of tests will be called.

    * Testing the cookbook module import is done implicitely as every test requires that it can be imported.
    * Testing for the presence of get_runner() in a class API cookbook is implicit if any test instantiates the
      cookbook object, e.g. the test_argument_parser test.

    """

    def _is_cookbook_class(self, name, obj):
        return inspect.isclass(obj) and issubclass(obj, CookbookBase) and not name.endswith("Base")

    def test_owner_team(self):
        """If set the owner_team should have one of the valid values."""
        for name, obj in inspect.getmembers(self.module):
            # Module API check
            if name == "__owner_team__":
                assert obj in VALID_OWNER_TEAM, (f"Module {self.module} has an invalid __owner_team__ '{obj}'. "
                                                 f"It must be one of:\n{VALID_OWNER_TEAM_STR}")

            # Class API check, includes base classes except CookbookBase
            if inspect.isclass(obj) and issubclass(obj, CookbookBase) and obj is not CookbookBase:
                owner = getattr(obj, "owner_team", None)
                if owner is None:
                    continue

                assert owner in VALID_OWNER_TEAM, (f"Class {self.module}.{obj.__name__} has an invalid owner_team "
                                                   f"'{owner}'. Must be one of:\n{VALID_OWNER_TEAM_STR}")

    def test_argument_parser(self):
        """It should return a valid ArgumentParser instance without raising exceptions."""
        mocked_spicerack = mock.MagicMock()
        for name, obj in inspect.getmembers(self.module):
            # Module API check
            if name == "argument_parser":
                parser = obj()
                assert isinstance(parser, argparse.ArgumentParser)

            # Class API check
            if self._is_cookbook_class(name, obj):
                cookbook_obj = obj(mocked_spicerack)
                parser = cookbook_obj.argument_parser()
                assert isinstance(parser, argparse.ArgumentParser)

    def test_has_run(self):
        """If the cookbook uses the module API it should have a `run()` function."""
        for name, obj in inspect.getmembers(self.module):
            # Skip __init__.py files
            if self.module.__file__.endswith("__init__.py"):
                return

            # Module API check
            if name == "run":
                assert inspect.getfullargspec(self.module.run).args == ['args', 'spicerack']
                return

            # Skip class API cookbooks
            if self._is_cookbook_class(name, obj):
                return

        assert False, f"Module API cookbook {self.module.__name__} doesn't have a run() function."

    def test_class_argparse_tunable(self):
        """If the class properties for tuning argparse are set, they should be either True, False or None."""
        for name, obj in inspect.getmembers(self.module):
            # Skip class API cookbooks
            if self._is_cookbook_class(name, obj):
                for property_name in ("argument_reason_required", "argument_task_required"):
                    if hasattr(obj, property_name):
                        assert getattr(obj, property_name) in (None, True, False)
