"""Manage TLS certs on network devices."""

import datetime
import json
import logging
import shlex

from socket import gaierror

from ssl import get_server_certificate, SSLError
from subprocess import CalledProcessError, run
from typing import Optional

from cryptography import x509
from cryptography.x509 import Certificate
from cryptography.x509.oid import NameOID

from wmflib.interactive import ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.sre.network import parse_results

NETWORK_ROLES = ('cloudsw', 'asw', 'cr')
RENEW_EXPIRATION_DELTA = datetime.timedelta(weeks=4)
logger = logging.getLogger(__name__)


class Tls(CookbookBase):
    """Create or update a Junos or SONiC device's TLS certificate for TLS based management.

    Usage example:
        cookbook sre.network.tls lsw1-e8-eqiad
        cookbook sre.network.tls all (TODO)

    TODO maybe later add the CA to the bundle, but that might enable mTLS on some platforms

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('device', help='Short hostname (or all).')
        parser.add_argument('--system', action='store_true', help="No ensure_shell_is_durable.")
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return TlsRunner(args, self.spicerack)


class TlsRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the TLS certificate management runner and do pre-run checks."""
        self.verbose = spicerack.verbose
        self.dry_run = spicerack.dry_run
        self.remote = spicerack.remote()
        self.device = args.device
        if not self.dry_run and not args.system:
            ensure_shell_is_durable()

        self.netbox = spicerack.netbox()
        # TODO implement "any" selector
        self.netbox_device = self.netbox.api.dcim.devices.get(name=self.device)
        if not self.netbox_device:
            raise RuntimeError(f'{self.device}: device not in Netbox.')
        if self.netbox_device.role.slug not in NETWORK_ROLES:
            raise RuntimeError(f'{self.device}: invalid role, must be one of {NETWORK_ROLES}.')
        try:
            self.device_fqdn = self.netbox_device.primary_ip.dns_name
        except AttributeError as exc:
            raise RuntimeError(f'{self.device}: Missing primary IP in Netbox.') from exc
        if not self.device_fqdn:
            raise RuntimeError(f'{self.device}: Missing DNS name (FQDN) on primary IP in Netbox.')
        self.remote_host = self.remote.query('D{' + self.device_fqdn + '}')
        if self.netbox_device.device_type.manufacturer.slug == 'dell':
            self.platform = 'sonic'
            self.port = 8080
        elif self.netbox_device.device_type.manufacturer.slug == 'juniper':
            self.platform = 'junos'
            self.port = 32767
        else:
            raise RuntimeError(f'{self.device}: invalid manufacturer, must be dell or juniper.')

        try:
            puppet_conf_raw = run(shlex.split("puppet config print --render-as json"),
                                  capture_output=True,
                                  text=True,
                                  check=True)
            self.puppet_conf = json.loads(puppet_conf_raw.stdout)
        except CalledProcessError as exc:
            raise RuntimeError("Can't read Puppet's config.") from exc

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"for network device {self.device}"

    def run(self):
        """Required by Spicerack API."""
        new_cert_bundle = {}
        # Get the certificate exposed by the host (if any)
        cert = self.get_cert()

        # If there is a cert (eg. self signed) or no cert, or nothing listening
        if self.need_initial_setup(cert):
            new_cert_bundle = self.generate_new_cert()
        elif self.need_cert_refresh(cert):
            new_cert = self.refresh_cert()
            if new_cert:  # If we can refresh the cert from the CSR
                new_cert_bundle['cert'] = new_cert
            else:  # If we can't (eg. CSR missing), then start over
                logger.info("%s: Missing CSR on the device, generating a new one.", self.device)
                new_cert_bundle = self.generate_new_cert()

        if new_cert_bundle:
            logger.info("%s: ⚙️ Deploy needed.", self.device)
            if self.platform == 'sonic':
                self.deploy_cert_sonic(new_cert_bundle)
            elif self.platform == 'junos':
                self.deploy_cert_junos(new_cert_bundle)
            else:
                raise RuntimeError(f'{self.device}: Unsupported platform {self.platform}')
            logger.info("%s: 👍 All done.", self.device)
        else:
            logger.info("%s: 👍 Nothing to do.", self.device)

    def get_cert(self) -> Optional[Certificate]:
        """Query a TLS endpoint and return its certificate."""
        try:
            # TODO add the "timeout" parameter once cumin hosts are running python >= 3.10 to speed things up
            cert_pem = get_server_certificate((self.device_fqdn, self.port))
            return x509.load_pem_x509_certificate(str.encode(cert_pem))
        except (ConnectionRefusedError, gaierror, TimeoutError, SSLError):
            logger.info("%s: ❌ Can't connect to device, assuming initial bootstrap.", self.device)
            return None

    def need_initial_setup(self, cert_x509: Optional[Certificate]) -> bool:
        """Check a certificate and return True if it needs to be renewed."""
        if not cert_x509:
            return True
        try:
            cert_org_name = cert_x509.issuer.get_attributes_for_oid(NameOID.ORGANIZATION_NAME)[0].value
            cert_name = cert_x509.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
        except IndexError:
            logger.info('%s: 🟡 Certificate missing org or common name.', self.device)
            return True
        if cert_org_name != 'Wikimedia Foundation, Inc':
            logger.info('%s: 🟡 Certificate not generated by WMF (%s).', self.device, cert_org_name)
            return True
        if cert_name != self.device_fqdn:
            logger.info("%s: 🟡 Certificate doesn't match device FQDN (%s vs. %s).", self.device,
                        cert_name,
                        self.device_fqdn)
            return True
        return False

    def need_cert_refresh(self, cert_x509: Optional[Certificate]) -> bool:
        """Check a certificate and return if it needs to be renewed"""
        if not cert_x509:
            return True
        if datetime.datetime.now() > cert_x509.not_valid_after:
            logger.info("%s: 🕔 Certificate expired. refresh needed.",
                        self.device)
            return True
        if datetime.datetime.now() + RENEW_EXPIRATION_DELTA > cert_x509.not_valid_after:
            logger.info("%s: 🕔 Certificate expires in less than %s. refresh needed.",
                        self.device, RENEW_EXPIRATION_DELTA)
            return True
        logger.debug("Certificate expires on %s. no refresh needed.", cert_x509.not_valid_after)
        return False

    def _cfssl_command(self, operation: str, data_in: str):
        """Helper function for CFSSL actions."""
        logger.info("%s: 🔏 cfssl called with operation: %s.", self.device, operation)
        logger.debug("cfssl called with data: %s", data_in)
        command = (f"cfssl {operation} -config /etc/cfssl/client-cfssl.conf "
                   f"-tls-remote-ca {self.puppet_conf['localcacert']} "
                   f"-mutual-tls-client-cert /etc/cfssl/mutual_tls_client_cert.pem "
                   f"-mutual-tls-client-key {self.puppet_conf['hostprivkey']} "
                   "-label network_devices  -")
        try:
            cfssl_raw = run(shlex.split(command), capture_output=True, text=True, check=True, input=data_in)
            # logger.debug("cfssl output: %s.", str(cfssl_raw.stdout)) - T342079
            return json.loads(cfssl_raw.stdout)
        except CalledProcessError as exc:
            raise RuntimeError(f"{self.device}: CFSSL error while generating certificate:\n{exc.stderr}") from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{self.device}: Couldn't JSON parse CFSSL output:\n{exc}") from exc

    def generate_new_cert(self) -> dict:
        """Generate a new CSR/key/CA/cert bundle."""
        csr_json = json.dumps({"CN": self.device_fqdn,
                               "hosts": [self.device_fqdn],
                               "key": {"algo": "ecdsa", "size": 256},
                               "names": []
                               })
        return self._cfssl_command('gencert', csr_json)

    def refresh_cert(self) -> str:
        """Generate a new cert from an existing CSR."""
        csr = self.fetch_csr()
        if not csr:  # If empty string
            return csr  # return it for further processing
        return self._cfssl_command('sign', csr)['cert']

    def fetch_csr(self) -> str:
        """Fetch a CSR from an already configured device."""
        if self.platform == "junos":
            results_raw = self.remote_host.run_sync("file show /var/preserve/csr.pem",
                                                    print_output=self.verbose,
                                                    print_progress_bars=False,
                                                    is_safe=True)
        else:
            results_raw = self.remote_host.run_sync("cat ~/csr.pem",
                                                    print_output=self.verbose,
                                                    print_progress_bars=False,
                                                    is_safe=True)
        result_parsed = parse_results(results_raw)
        logger.debug("Content of the device's stored CSR: %s.", result_parsed)
        if isinstance(result_parsed, str):
            if 'error:' in result_parsed:
                return ''
            return result_parsed
        raise RuntimeError(f'{self.device}: Invalid data returned when trying to fetch CSR.')

    def _copy_to_junos(self, data: str, path: str, append: bool = False):
        """Helper function to write to Junos filesystem."""
        # The line below is to work around Junos limitation and lack of scp support in spicerack
        data_one_line = data.replace('\n', '\\n')
        if not append:
            # Try to delete the file just in case
            self.remote_host.run_sync(f"file delete {path}",
                                      print_output=self.verbose, print_progress_bars=False)
        # Another Junos limitation to this hack, ~1000 characters limit
        for part in [data_one_line[i:i + 900] for i in range(0, len(data_one_line), 900)]:
            self.remote_host.run_sync(f"start shell sh command \"echo -e '{part}' >> {path}\"",
                                      print_output=self.verbose, print_progress_bars=False)
        logger.debug("Written to %s (append: %s)\n%s", path, str(append), data)

    def deploy_cert_junos(self, cert_bundle):
        """Deploy the needed files on Junos."""
        if 'csr' in cert_bundle:  # Store the CSR in the a dir that survives upgrades/cleanup
            self._copy_to_junos(cert_bundle['csr'], '/var/preserve/csr.pem')

        self._copy_to_junos(cert_bundle['cert'], '/var/tmp/cert.pem')  # nosec - hardcoded_tmp_directory

        if 'key' in cert_bundle:
            # On junos both the client cert and key are in the same file
            self._copy_to_junos(cert_bundle['key'], '/var/tmp/cert.pem', append=True)  # nosec - hardcoded_tmp
            # But we also need to keep the key somewhere to re-use it during a refresh
            self._copy_to_junos(cert_bundle['key'], '/var/preserve/key.pem', append=False)
        else:  # refresh
            self.remote_host.run_sync("start shell sh command \"cat /var/preserve/key.pem >> /var/tmp/cert.pem\"",
                                      print_output=self.verbose, print_progress_bars=False)

        self.remote_host.run_sync((f"configure;set security certificates local {self.device_fqdn.split('.')[0]}-cert "
                                   "load-key-file /var/tmp/cert.pem;commit"),
                                  print_output=self.verbose, print_progress_bars=False)
        self.remote_host.run_sync("file delete /var/tmp/cert.pem",
                                  print_output=self.verbose, print_progress_bars=False)
        logger.debug("Client certificate and key loaded on device.")

    def deploy_cert_sonic(self, cert_bundle):
        """Deploy the needed files on SONiC."""
        logger.debug("Deploying everything on SONiC.")
        if 'csr' in cert_bundle:
            # Store the CSR in the a dir that survives upgrades/cleanup
            self.remote_host.run_sync(f"echo '{cert_bundle['csr']}' > ~/csr.pem",
                                      print_output=self.verbose, print_progress_bars=False)
        self.remote_host.run_sync(f"echo '{cert_bundle['cert']}' | sudo tee /etc/sonic/cert/host/default.crt",
                                  print_output=self.verbose, print_progress_bars=False)
        if 'key' in cert_bundle:
            self.remote_host.run_sync(f"echo '{cert_bundle['key']}' | sudo tee /etc/sonic/cert/keys/default.key",
                                      print_output=self.verbose, print_progress_bars=False)
        self.remote_host.run_sync("sudo service telemetry restart",
                                  print_output=self.verbose, print_progress_bars=False)
        logger.debug("Telemetry deamon restarted with new certificate.")
