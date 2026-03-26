import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation

logger = logging.getLogger(__name__)

HOSTS = "A:titan"
STOP_CMD = "/usr/bin/systemctl stop thanos-compact"
CHECK_CMD = '! (/usr/bin/ps faux | /usr/bin/grep -q "[t]hanos compact")'
COPY_CMD = "/usr/bin/cp --preserve=all /etc/thanos-compact/relabel.yaml.unreferenced /etc/thanos-compact/relabel.yaml"
START_CMD = (
    "if /usr/bin/systemctl is-enabled --quiet thanos-compact; then /usr/bin/systemctl start thanos-compact; else echo 'thanos compact unit is not enabled'; fi" # noqa E501
)


class ThanosCompactRestart(CookbookBase):
    """Thanos Compact service restart

    Usage example:
        cookbook sre.o11y.thanos-compact-restart --reason 'new relabel config file' --patch-id 111111 --task-id T12345
    """

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "--patch-id", required=True, help="Patch id (for confirmation)"
        )
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return ThanosCompactRestartRunner(args, self.spicerack)


class ThanosCompactRestartRunner(CookbookRunnerBase):
    def __init__(self, args, spicerack):
        self.args = args
        self.spicerack = spicerack
        self.admin_reason = self.spicerack.admin_reason(
            f"{self.args.reason} (patch id: {self.args.patch_id})",
            task_id=self.args.task_id,
        )
        self.remote_hosts = self.spicerack.remote().query(HOSTS)
        self.puppet = self.spicerack.puppet(self.remote_hosts)


    @property
    def runtime_description(self):
        return self.admin_reason.reason

    def run(self):
        """Required by Spicerack API."""

        ask_confirmation(f"Have you merged the patch {self.args.patch_id}?")
        self.puppet.run()

        with self.puppet.disabled(self.admin_reason):
            self.remote_hosts.run_sync(STOP_CMD)

            self.remote_hosts.run_sync(CHECK_CMD, is_safe=True)

            self.remote_hosts.run_sync(COPY_CMD)

            self.remote_hosts.run_sync(START_CMD)
