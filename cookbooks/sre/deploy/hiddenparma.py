"""Deploy the hiddenparma web application."""

import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs

from cookbooks.sre.deploy import DEPLOYMENT_CNAME

logger = logging.getLogger(__name__)


class HiddenParma(CookbookBase):
    """Hiddenparma deployment cookbook.

    Mostly uses the sre.deploy.python-code cookbook to deploy the hiddenparma application,
    but also updates the deploy repo on the deployment host and restarts the service.

    Usage example:
        cookbook sre.deploy.hiddenparma
        cookbook sre.deploy.hiddenparma -r 'some reason' -t T12345
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "-r",
            "--reason",
            help=("An optional reason for the deployment. Username/host are added automatically."),
        )
        parser.add_argument(
            "-t", "--task-id", help="An optional task ID to refer in the downtime message (i.e. T12345)."
        )

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return HiddenParmaRunner(args, self.spicerack)


class HiddenParmaRunner(CookbookRunnerBase):
    """Hiddenparma deployment runner class."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.args = args
        self.spicerack = spicerack
        self.deployment_host = self.spicerack.remote().query(spicerack.dns().resolve_cname(DEPLOYMENT_CNAME))
        self.deploy_repo_dir = "/srv/deployment/hiddenparma/deploy"
        self.reason = args.reason if args.reason else "[not really into teleological thinking]"

    @property
    def lock_args(self):
        """Make the cookbook lock exclusive."""
        return LockArgs(suffix="hiddenparma", concurrency=1, ttl=1800)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the downtime action."""
        reason = self.spicerack.admin_reason(self.reason, task_id=self.args.task_id)
        return f'Hiddenparma deployment to the alerting hosts with reason: "{reason}"'

    def run(self):
        """Run the cookbook."""
        logger.info("Pulling the latest changes from the deployment repository")
        self.deployment_host.run_sync(
            f"runuser -u mwdeploy -- /usr/bin/git -C {self.deploy_repo_dir} pull", print_progress_bars=False
        )
        self.deployment_host.run_sync(
            f"runuser -u mwdeploy -- /usr/bin/git -C {self.deploy_repo_dir} submodule update --init --recursive",
            print_progress_bars=False,
        )
        args = ["-r", self.reason]
        if self.args.task_id:
            args.extend(["-t", self.args.task_id])
        args.extend(["hiddenparma", "A:icinga"])
        logger.info("Running the deployment")
        exit_code = self.spicerack.run_cookbook("sre.deploy.python-code", args=args)
        if exit_code != 0:
            raise RuntimeError("Deployment failed")
        logger.info("Deployment successful")
        logger.info("Restarting the service")
        self.spicerack.remote().query("A:icinga").run_sync("systemctl restart hiddenparma.service")
