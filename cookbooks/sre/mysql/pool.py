"""Pool or depool a DB from dbctl."""
from datetime import datetime, timedelta
from pprint import pformat
from time import sleep
import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.decorators import retry
from wmflib.interactive import ensure_shell_is_durable

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


logger = logging.getLogger(__name__)


# This class is also used as a base class for the Depool cookbook
class Pool(CookbookBase):
    """Pool a DB instance in dbctl and allow to gradually increase its pooled percentage.

    There are three available profiles to control the repool steps. All of them use a power of two progression for
    increasing the percentage from 0% to 100%.

    The default profile does it in 4 steps. There are also a fast profile with just 2 steps and a slow one with 10
    steps.

    The current sleep between steps is 15 minutes.

    Examples:
        # Pool the instance gradually sleeping in between steps
        sre.mysql.pool -r "Some reason" db1001

        # Pool the instance and update a Phabricator task at the start and end of the pooling operation
        sre.mysql.pool -r "Some reason" -t T12345 db1001

        # Pool the instance quickly with just two steps
        sre.mysql.pool -r "Some reason" --fast db1001

    """

    def argument_parser(self):
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("-r", "--reason", required=True, help="The administrative reason for the action.")
        parser.add_argument("-t", "--task-id", help="The Phabricator task ID to update and refer (i.e.: T12345)")
        if self.__class__.__name__ == "Pool":
            profile = parser.add_mutually_exclusive_group()
            profile.add_argument("--fast", action="store_true", help="Repool the host quicker with just two steps.")
            profile.add_argument("--slow", action="store_true", help="Repool the host more slowly, with ten steps.")

        # TODO: add support for multiple instances? Based on what? (puppetdb, dbctl, orchestrator)
        parser.add_argument("instance", help="Instance name as defined in dbctl.")

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        args.operation = self.__class__.__name__.lower()
        return PoolDepoolRunner(args, self.spicerack)


class PoolDepoolRunner(CookbookRunnerBase):
    """Pool or depool a MySQL instance cookbook runner."""

    def __init__(self, args, spicerack):
        """As specified by Spicerack API."""
        # Silence some more noisy loggers for the dry-run mode
        logging.getLogger("etcd.client").setLevel(logging.INFO)
        logging.getLogger("conftool").setLevel(logging.INFO)

        self.args = args
        self.pool = args.operation == "pool"
        self.dbctl = spicerack.dbctl()
        self.reason = spicerack.admin_reason(args.reason, task_id=args.task_id)
        self.dry_run = spicerack.dry_run

        if self.pool:
            if self.args.slow:
                self.steps: tuple[int, ...] = (1, 4, 9, 16, 25, 36, 49, 64, 81, 100)  # 10 steps, power or 2 progression
            elif self.args.fast:
                self.steps = (25, 100)  # 2 steps, power of 2 progression
            else:
                self.steps = (6, 25, 56, 100)  # 4 steps, power of 2 progression

        instance = self.dbctl.instance.get(self.args.instance)
        if instance is None:
            raise RuntimeError(f"Unable to find instance {self.args.instance} in dbctl. Aborting.")

        self.datacenter = instance.tags.get("datacenter")
        self.phabricator = None
        if self.reason.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

        if self.pool:
            ensure_shell_is_durable()

        # TODO: improve handling of spurious changes, right now it bails out
        # TODO: check that the host is not downtimed and green in Icinga/AM?
        # TODO: check for mysql/metrics errors during the repooling operation?

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        suffix = ""
        if self.pool:
            adj = "slowly" if self.args.slow else "quickly" if self.args.fast else "gradually"
            suffix = f" {adj} with {len(self.steps)} steps"
        return f"{self.args.instance}{suffix} - {self.reason.reason}"

    @property
    def lock_args(self):
        """Make the cookbook lock per-instance."""
        # TTL includes both the sleep time (900s) plus the potential retries for wait_diff_clean (30*30s) for each step
        return LockArgs(suffix=self.args.instance, concurrency=1, ttl=1800 * len(self.steps) if self.pool else 60)

    def check_action_result(self, action_result, message):
        """Raise on failure and log any messages present in an ActionResult instance."""
        for result_message in action_result.messages:
            logger.log(logging.INFO if action_result.success else logging.ERROR, result_message)

        if action_result.announce_message:
            logger.info(action_result.announce_message)

        if not action_result.success:
            raise RuntimeError(f"Failed to {message}")

    def run(self):
        """Required by the Spicerack API."""
        if self.pool:
            if self.phabricator is not None:
                self.phabricator.task_comment(
                    self.reason.task_id, f"Start pool of {self.runtime_description} - {self.reason.owner}")

            self.gradual_pooling()

        else:
            message = "depool instance {self.args.instance}"
            self.wait_diff_clean()
            ret = self.dbctl.instance.depool(self.args.instance)
            self.check_action_result(ret, message)
            self.commit_change(message)

        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.reason.task_id,
                f"Completed {self.args.operation} of {self.runtime_description} - {self.reason.owner}",
            )

    def gradual_pooling(self):
        """Gradually pool the instance with increasing percentages."""
        sleep_duration = 5 if self.dry_run else 900
        for percentage in self.steps:

            instance = self.dbctl.instance.get(self.args.instance)
            current_pooling = {(section['pooled'], section['percentage'] >= percentage)
                               for section in instance.sections.values()}
            # Skip if all the sections are pooled with a percentage equal or greater than the percentage to set
            if len(current_pooling) == 1 and current_pooling.pop() == (True, True):
                logger.info("Skipping pooling instance %s at %d%%, instance already pooled with higher percentage",
                            self.args.instance, percentage)
                continue

            message = f"pool instance {self.args.instance} at {percentage}%"
            logger.info(message)
            self.wait_diff_clean()
            ret = self.dbctl.instance.pool(self.args.instance, percentage=percentage)
            self.check_action_result(ret, message)
            self.commit_change(message)
            if percentage != 100:
                sleep_ends = datetime.utcnow() + timedelta(seconds=sleep_duration)
                logger.info("Sleeping for %ds, next step will be at %s", sleep_duration, sleep_ends)
                sleep(sleep_duration)  # TODO: replace with a polling of metrics from prometheus or the DB itself

    def commit_change(self, message):
        """Check the diff and commit the change."""
        ret, diff = self.get_diff()
        self.check_action_result(ret, f"get diff to {message}")

        self.check_diff(diff)

        ret = self.dbctl.config.commit(batch=True, datacenter=self.datacenter, comment=self.reason.reason)
        self.check_action_result(ret, f"commit change to {message}")

    @retry(
        tries=30,
        delay=timedelta(seconds=30),
        backoff_mode="constant",
        failure_message="Waiting for dbctl config diff to be clean",
        exceptions=(RuntimeError,),
    )
    def wait_diff_clean(self):
        """Poll until dbctl config diff is clean."""
        ret, _ = self.get_diff()
        if ret.success and ret.exit_code == 0:  # Empty diff
            return

        raise RuntimeError("dbctl config has a pending diff or unable to get the diff")

    def get_diff(self):
        """Get the current dbctl config diff."""
        ret, diff = self.dbctl.config.diff(datacenter=self.datacenter, force_unified=True)
        self.check_action_result(ret, "evaluate dbctl config diff")
        return ret, diff

    def check_diff(self, diff):
        """Ensure that the diff has only the expected change in it."""
        # Count the diff lines unrelated to the current change
        count = 0
        for line in diff:
            if (
                # ignore control lines and context lines
                any(line.startswith(prefix) for prefix in (" ", "---", "+++", "@@"))
                # ignore DB groups lines (e.g. '"vslow": {', '}')
                or any(substr in line for substr in ("{", "}"))
            ):
                continue

            if self.args.instance not in line:
                count += 1

        if count:
            logger.error("The current diff has %d spurious changes, aborting:\n%s", count, pformat(diff))
            raise RuntimeError("Unable to proceed due to spurious changes in the diff")
