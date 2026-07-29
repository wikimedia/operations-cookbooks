import logging

from cookbooks.sre.mysql import (
    YesNo,
)
from pytest import (
    fixture,
    raises,
)


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


def test_yesno(mocker, caplog) -> None:
    for val in (1, "1", "y", "yes", "true"):
        assert bool(YesNo(val)) is True

    for val in (0, "0", "n", "no", "false"):
        assert bool(YesNo(val)) is False

    for val in ("a", 2):
        with raises(ValueError):
            YesNo(val)
