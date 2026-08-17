"""
MariaDB host decommissioning helper.
- Performs safety checks to avoid decommissioning masters or candidate masters
- Drives task creation and puppet patches
- Depools the host
- Removes the host from Zarcillo
- Removes the host from Orchestrator
- Updates Phabricator

This cookbook is meant for the Data Persistence team.
"""

import getpass
import logging
import os
import sys
from argparse import ArgumentParser

import requests
from cookbooks.sre.mysql import Phabricator, ZarcilloClient, init_step, phabricator_client
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

log = logging.getLogger(__name__)
step = init_step(__name__)


def _fetch_instances(spicerack, hn: str) -> list:
    zc = ZarcilloClient(spicerack)
    instances = zc.fetch_instances_meta(hn)
    if instances == []:
        log.error(f"Could not find any instance for {hn} on Zarcillo")
        sys.exit(1)

    log.info(f"instances on {hn}:")
    for i in instances:
        log.info(f"    role: {i.role} port: {i.port}")

    if any(i.preferred_candidate for i in instances):
        log.error(f"{hn} is a preferred candidate!")
        sys.exit(1)

    if any(i.role == "master" for i in instances):
        log.error(f"{hn} is a master!")
        sys.exit(1)

    return instances


def _delete_from_zarcillo(hostname: str) -> None:
    try:
        username = os.getlogin()
    except OSError:
        username = getpass.getuser()

    headers = {"X-WMF-Username": username}
    url = "https://zarcillo.wikimedia.org/api/v1/delete_host"
    zarc_res = requests.post(url, json={"hostname": hostname}, headers=headers, timeout=20)
    zarc_res.raise_for_status()
    err = zarc_res.json().get("errmsg")
    if err:
        err = f"Zarcillo API error: {err}"
        log.error(err)
        raise RuntimeError(err)


def diy(s: str) -> None:
    log.info(f"▶ {s}")


class Decommission(CookbookBase):
    argument_task_required = True

    def argument_parser(self) -> ArgumentParser:
        ap = super().argument_parser()
        ap.add_argument("hostname")
        return ap

    def get_runner(self, args) -> CookbookRunnerBase:
        return DecommissionRunner(args, self.spicerack)


class DecommissionRunner(CookbookRunnerBase):
    def __init__(self, args, spicerack):
        self.args = args
        self.spicerack = spicerack

    @staticmethod
    def puppet_hiera_file_exists(hostname: str) -> bool:
        url = f"https://gerrit.wikimedia.org/r/projects/operations%2Fpuppet/branches/production/files/hieradata%2Fhosts%2F{hostname}.yaml/content"
        resp = requests.get(url, timeout=20)
        return resp.status_code == 200

    @staticmethod
    def update_phab_desc_tick_boxes(desc: str) -> str:
        """Tick service-owner boxes"""
        lines = desc.splitlines()
        inside = False
        new_lines = []

        for line in lines:
            if not inside:
                if "Steps for service owner" in line:
                    inside = True

            else:
                if "End service owner steps" in line:
                    inside = False

                elif line.startswith("[] -"):
                    line = "[x] -" + line[4:]

                elif line.startswith("[ ] -"):
                    line = "[x] -" + line[5:]

            new_lines.append(line)

        return "\n".join(new_lines)

    def handover_task_to_dcops(self, task_id: str, dc: str) -> None:
        """Update task
        - unassign
        - replace tags with: dc-ops, ops-<dc> and DBA, decommission-hardware
        - tick boxes in the service owner section
        - add comment
        """
        t = self.phab.fetch_task(task_id)
        new_desc = self.update_phab_desc_tick_boxes(t.description)
        project_tags = ["dc-ops", f"ops-{dc.lower()}", "DBA", "decommission-hardware"]
        cm = "This host is ready for DC-Ops to decommission"
        t.unassign().set_project_tags(project_tags).add_comment(cm).set_description(new_desc)

        if self.spicerack.dry_run:
            log.info(f"DRY-RUN: not running phabricator transaction: {t._pending_txns}")
        else:
            t.send()
            log.info("Phabricator task updated")

    def run(self):
        ensure_shell_is_durable()
        spicerack = self.spicerack
        dbctl = spicerack.dbctl()
        hostname: str = self.args.hostname
        task_id: str = self.args.task_id

        instances = _fetch_instances(spicerack, hostname)
        fqdn = instances[0].fqdn
        dc = instances[0].dc

        # dbctl contains only instances on port 3306, indexed by hostname only
        is_on_dbctl = dbctl.instance.get(hostname) is not None
        if not is_on_dbctl:
            ask_confirmation("The host is not on dbctl. Continue?")

        phab: Phabricator = phabricator_client(spicerack)
        self.phab = phab

        if is_on_dbctl:
            step("depool", "depooling host if needed")
            spicerack.run_cookbook("sre.mysql.depool", ["--task-id", task_id, "-r", "Decommission", hostname])

            diy(f"Create a puppet patch to remove {hostname} from dbctl")
            diy("example: https://gerrit.wikimedia.org/r/c/operations/puppet/+/638343")
            diy("Review and merge and then run:")
            diy("ssh puppetserver1001.eqiad.wmnet -t sudo -i puppet-merge")
            ask_confirmation("Have you puppet-merged?")

            diy("Then run:")
            diy(f"sudo dbctl config commit -m 'Remove {hostname} from dbctl {task_id}'")
            ask_confirmation("Done?")

            while dbctl.instance.get(hostname):
                log.warning(f"{hostname} is still showing in dbctl: review manual steps.")
                if spicerack.dry_run:
                    break
                ask_confirmation("Check again?")

        step("run_decommission", "Running decommission cookbook")
        spicerack.run_cookbook("sre.hosts.decommission", ["-t", task_id, fqdn])

        diy(f"Create a puppet patch to remove {hostname} from Puppet entirely")
        diy("example: https://gerrit.wikimedia.org/r/c/operations/puppet/+/1286167")
        diy("Review and merge and then run:")
        diy("ssh puppetserver1001.eqiad.wmnet -t sudo -i puppet-merge")
        ask_confirmation("Done?")

        while self.puppet_hiera_file_exists(hostname):
            log.warning(f"hieradata/hosts/{hostname}.yaml file is still in Puppet: review manual steps.")
            if spicerack.dry_run:
                break
            ask_confirmation("Check again?")

        step("zarcillo_cleanup", "Removing host from zarcillo")
        spicerack.sal_logger.info(f"Removing {hostname} from zarcillo {task_id}")
        if not spicerack.dry_run:
            _delete_from_zarcillo(hostname)

        phab.task_comment(task_id, f"{hostname} has been deleted from zarcillo")

        step("orchestrator", "Running orchestrator cleanup")
        orc = spicerack.remote().query("dborch1002.wikimedia.org")
        for i in instances:
            cmd = f"orchestrator -c forget -i {i.hostname}:{i.port}"
            log.debug(f"Running on orchestrator: {cmd}")
            orc.run_sync(cmd)

        step("phabricator", "Update phabricator task")
        phab.task_comment(task_id, f"{hostname} has been decommissioned by Data Persistence")

        log.info("-" * 80)
        step("handover", "Update the task and send it to dcops")
        self.handover_task_to_dcops(task_id, dc)
