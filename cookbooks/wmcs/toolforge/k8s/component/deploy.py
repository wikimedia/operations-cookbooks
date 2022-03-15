r"""WMCS Toolforge Kubernetes - deploy a kubernetes custom component

Usage example: \
    cookbook wmcs.toolforge.k8s.component.deploy \
        --git-url https://gerrit.wikimedia.org/r/cloud/toolforge/jobs-framework-api \
"""
import argparse
import random
import string
import logging
from typing import List

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CommonOpts, SALLogger, add_common_opts, run_one, with_common_opts

LOGGER = logging.getLogger(__name__)


class ToolforgeComponentDeploy(CookbookBase):
    """Deploy a kubernetes custom component in Toolforge."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser, project_default="toolsbeta")
        parser.add_argument(
            "--deploy-node-hostname",
            required=False,
            default="toolsbeta-test-k8s-control-4",
            help="k8s control node hostname",
        )
        parser.add_argument(
            "--git-url",
            required=True,
            help="git URL for the source code",
        )
        parser.add_argument(
            "--git-name",
            required=False,
            help="git repository name. If not provided, it will be guessed based on the git URL",
        )
        parser.add_argument(
            "--git-branch",
            required=False,
            default="main",
            help="git branch in the source repository",
        )
        parser.add_argument(
            "--deployment-command",
            required=False,
            help="command to trigger the deployment. If not provided, it will be kubectl apply -k deployment/project",
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, ToolforgeComponentDeployRunner,)(
            deploy_node_hostname=args.deploy_node_hostname,
            git_url=args.git_url,
            git_name=args.git_name,
            git_branch=args.git_branch,
            deployment_command=args.deployment_command,
            spicerack=self.spicerack,
        )


def _randomword(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))  # nosec


def _sh_wrap(cmd: str) -> List[str]:
    return ["/bin/sh", "-c", "--", f"'{cmd}'"]


class ToolforgeComponentDeployRunner(CookbookRunnerBase):
    """Runner for ToolforgeComponentDeploy."""

    def __init__(
        self,
        common_opts: CommonOpts,
        deploy_node_hostname: str,
        git_url: str,
        git_name: str,
        git_branch: str,
        deployment_command: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.deploy_node_hostname = deploy_node_hostname
        self.git_url = git_url
        self.git_name = git_name
        self.git_branch = git_branch
        self.deployment_command = deployment_command
        self.spicerack = spicerack
        self.random_dir = f"/tmp/cookbook-toolforge-k8s-component-deploy-{_randomword(10)}"  # nosec
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

        if not self.git_name:
            self.git_name = self.git_url.split("/")[-1]
            LOGGER.info("INFO: guesses git tree name as %s", self.git_name)

        if not self.deployment_command:
            self.deployment_command = f"kubectl apply -k deployment/{common_opts.project}"
            LOGGER.info("INFO: guesses deployment command as %s", self.deployment_command)

    def run(self) -> None:
        """Main entry point"""
        remote = self.spicerack.remote()
        deploy_node_fqdn = f"{self.deploy_node_hostname}.{self.common_opts.project}.eqiad1.wikimedia.cloud"
        deploy_node = remote.query(f"D{{{deploy_node_fqdn}}}", use_sudo=True)
        LOGGER.info("INFO: using deploy node %s", deploy_node_fqdn)

        # create temp dir
        LOGGER.info("INFO: creating temp dir %s", self.random_dir)
        run_one(node=deploy_node, command=["mkdir", self.random_dir], print_output=False, print_progress_bars=False)

        # git clone
        cmd = f"cd {self.random_dir} ; git clone {self.git_url}"
        LOGGER.info("INFO: git cloning %s", self.git_url)
        run_one(node=deploy_node, command=_sh_wrap(cmd), print_output=False, print_progress_bars=False)

        # git checkout branch
        repo_dir = f"{self.random_dir}/{self.git_name}"
        cmd = f"cd {repo_dir} ; git checkout {self.git_branch}"
        LOGGER.info("INFO: git checkout branch '%s' on %s", self.git_branch, repo_dir)
        run_one(node=deploy_node, command=_sh_wrap(cmd), print_output=False, print_progress_bars=False)

        # get git hash for the SAL logger
        cmd = f"cd {repo_dir} ; git rev-parse --short HEAD"
        git_hash = run_one(
            node=deploy_node, command=_sh_wrap(cmd), last_line_only=True, print_output=False, print_progress_bars=False
        )

        # deploy!
        cmd = f"cd {repo_dir} ; {self.deployment_command}"
        LOGGER.info("INFO: deploying with %s", self.deployment_command)
        run_one(node=deploy_node, command=_sh_wrap(cmd), print_output=False, print_progress_bars=False)

        # cleanup
        cmd = f"rm -rf --preserve-root=all {self.random_dir}"
        LOGGER.info("INFO: cleaning up temp dir %s", self.random_dir)
        run_one(node=deploy_node, command=cmd.split(), is_safe=False, print_output=False, print_progress_bars=False)

        self.sallogger.log(message=f"deployed kubernetes component {self.git_url} ({git_hash})")
