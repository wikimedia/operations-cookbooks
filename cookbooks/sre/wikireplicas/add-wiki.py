"""Coobkook for adding a new wiki to the replicas."""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

logger = logging.getLogger(__name__)


class AddWiki(CookbookBase):
    """Add a new wiki to the Wiki Replicas.

    Usage example:
        cookbook sre.wikireplicas.add-wiki --task-id T12345 zhuwikisource
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("--task-id", required=True, help="task_id for the change")
        parser.add_argument(
            "--skip-dns",
            action="store_true",
            help="If the dns step is already done, skip it",
        )
        parser.add_argument(
            "database",
            metavar="<database_name>",
            type=str,
            help="Name of the wiki DB",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """As specified by Spicerack API."""
        return AddWikiRunner(
            spicerack=self.spicerack,
            database=args.database,
            task_id=args.task_id,
            skip_dns=args.skip_dns,
        )


class AddWikiRunner(CookbookRunnerBase):
    """Coobkook runner class for adding a new wiki to the replicas."""

    def __init__(self, spicerack: Spicerack, database: str, task_id: str, skip_dns: bool):
        """Initialize the runner."""
        self.spicerack = spicerack
        self.database = database
        self.task_id = task_id
        self.skip_dns = skip_dns

    def run(self):
        """Required by Spicerack API."""
        remote = self.spicerack.remote()
        replicas = remote.query("A:wikireplicas-all")
        s7_replicas = remote.query(
            "P{R:Profile::Mariadb::Section = 's7'} and P{P:wmcs::db::wikireplicas::mariadb_multiinstance}"
        )

        # Get a cloudcontrol host to run the DNS update on
        cloudcontrol = remote.query("A:cloudcontrol")
        control_host = next(cloudcontrol.split(len(cloudcontrol)))

        index_cmd = f"/usr/local/sbin/maintain-replica-indexes --database {self.database}"
        view_cmd = f"/usr/local/sbin/maintain-views --replace-all --databases {self.database}"
        meta_p_cmd = f"/usr/local/sbin/maintain-meta_p --databases {self.database}"
        wiki_dns_cmd = "source /root/novaenv.sh; wmcs-wikireplica-dns --aliases"
        logger.info("Generating views...")
        replicas.run_async(index_cmd, view_cmd)
        if not self.skip_dns:
            logger.info("Adding DNS")
            control_host.run_sync(wiki_dns_cmd)

        logger.info("Finalizing meta_p")
        s7_replicas.run_async(meta_p_cmd)
        self.spicerack.sal_logger.info("Added views for new wiki: %s %s", self.database, self.task_id)
