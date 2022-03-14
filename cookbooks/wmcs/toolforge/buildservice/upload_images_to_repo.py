r"""WMCS Toolforge - Upload the needed images for the buildservice to the toolforge repo

Usage example:
    cookbook wmcs.toolforge.buildservice.upload_images_to_repo \
        --tekton-version v0.33.2 \
        --bash-version 5.1.4 \
        --lifecycle-version 0.10.2

"""
# pylint: disable=too-many-arguments
import argparse
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteHosts

from cookbooks.wmcs import CommonOpts, SALLogger, add_common_opts, run_one, with_common_opts


class UploadImagesToRepo(CookbookBase):
    """Uploads the external buildservice images to the local toolforge repository for local comsumption."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--tekton-version",
            required=False,
            default=None,
            help="Tag for the tekton images to pull",
        )
        parser.add_argument(
            "--bash-version",
            required=False,
            default=None,
            help="Tag for the bash image to use.",
        )
        parser.add_argument(
            "--lifecycle-version",
            required=False,
            default=None,
            help="Tag for the buildpacks lifecycle image to use.",
        )
        parser.add_argument(
            "--image-repo-url",
            required=False,
            default="docker-registry.tools.wmflabs.org",
            help="Repository to upload the images to.",
        )
        parser.add_argument(
            "--uploader-node",
            required=False,
            default="tools-docker-imagebuilder-01.tools.eqiad1.wikimedia.cloud",
            help="Host to use to pull and push to the given repository.",
        )
        add_common_opts(parser, project_default="toolsbeta")

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, UploadImagesToRepoRunner)(
            tekton_version=args.tekton_version,
            lifecycle_version=args.lifecycle_version,
            bash_version=args.bash_version,
            image_repo_url=args.image_repo_url,
            uploader_node=args.uploader_node,
            spicerack=self.spicerack,
        )


def _update_image(uploader_node: RemoteHosts, pull_url: str, push_url: str) -> None:
    run_one(command=["docker", "pull", pull_url], node=uploader_node)
    run_one(command=["docker", "tag", pull_url, push_url], node=uploader_node)
    run_one(command=["docker", "push", push_url], node=uploader_node)


class UploadImagesToRepoRunner(CookbookRunnerBase):
    """Runner for UploadImagesToRepo."""

    TEKTON_COMMON_PATH = "gcr.io/tekton-releases/github.com/tektoncd/pipeline/cmd"
    TEKTON_IMAGES = [
        "controller",
        "entrypoint",
        "git-init",
        "imagedigestexporter",
        "kubeconfigwriter",
        "nop",
        "pullrequest-init",
        "webhook",
        "workingdirinit",
    ]

    def __init__(
        self,
        common_opts: CommonOpts,
        image_repo_url: str,
        uploader_node: str,
        tekton_version: Optional[str],
        lifecycle_version: Optional[str],
        bash_version: Optional[str],
        spicerack: Spicerack,
    ):
        """Init"""
        self.tekton_version = tekton_version
        self.lifecycle_version = lifecycle_version
        self.bash_version = bash_version
        self.image_repo_url = image_repo_url
        self.uploader_node = uploader_node
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def run(self) -> None:
        """Main entry point"""
        remote = self.spicerack.remote()
        uploader_node = remote.query(f"D{{{self.uploader_node}}}", use_sudo=True)

        if self.tekton_version:
            self.sallogger.log(message=f"Updating the tekton related images on {self.image_repo_url}")
            for image_name in self.TEKTON_IMAGES:
                pull_url = f"{self.TEKTON_COMMON_PATH}/{image_name}:{self.tekton_version}"
                push_url = f"{self.image_repo_url}/toolforge-tektoncd-pipeline-cmd-{image_name}:{self.tekton_version}"
                _update_image(uploader_node=uploader_node, pull_url=pull_url, push_url=push_url)

        if self.bash_version:
            self.sallogger.log(message=f"Updating the bash image on {self.image_repo_url}")
            pull_url = f"docker.io/library/bash:{self.bash_version}"
            push_url = f"{self.image_repo_url}/toolforge-library-bash:{self.bash_version}"
            _update_image(uploader_node=uploader_node, pull_url=pull_url, push_url=push_url)

        if self.lifecycle_version:
            self.sallogger.log(message=f"Updating the lifecycle image on {self.image_repo_url}")
            pull_url = f"docker.io/buildpacksio/lifecycle:{self.lifecycle_version}"
            push_url = f"{self.image_repo_url}/toolforge-buildpacksio-lifecycle:{self.lifecycle_version}"
            _update_image(uploader_node=uploader_node, pull_url=pull_url, push_url=push_url)

        # this image should not be pulled with a tag, so CRI-O can run it, so we update it always.
        self.sallogger.log(message=f"Updating the distroless/base image on {self.image_repo_url}")
        _update_image(
            uploader_node=uploader_node,
            pull_url="gcr.io/distroless/base",
            push_url=f"{self.image_repo_url}/toolforge-distroless-base",
        )
