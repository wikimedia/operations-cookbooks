"""Depool a DB from dbctl."""

from cookbooks.sre.mysql.pool import Pool


class Depool(Pool):
    """Depool a DB instance from dbctl.

    Examples:
        # Immediately depool the instance
        sre.mysql.newdepool -r "Some reason" db1001

        # Immediately depool the instance and update a Phabricator task
        sre.mysql.newdepool -r "Some reason" -t T12345 db1001

    """
