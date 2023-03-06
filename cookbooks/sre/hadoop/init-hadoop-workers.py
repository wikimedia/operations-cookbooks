"""Initialize an Hadoop worker"""
import logging
import string

from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteExecutionError


logger = logging.getLogger(__name__)


class InitHadoopWorkers(CookbookBase):
    """Initialize an Hadoop worker

    This cookbook helps the Analytics team in setting up new Hadoop
    workers, configuring the Hadoop disk partitions not currently
    handled by partman during d-i.
    """

    def argument_parser(self):
        """Argument parser helper function"""
        parser = super().argument_parser()
        parser.add_argument('hostname_pattern', help='The cumin hostname pattern of the Hadoop worker(s) '
                            'to initialize.', type=str)
        parser.add_argument('--disks-number', type=int, default=12,
                            help="The number of datanode disks/partitions to initialize.")
        parser.add_argument('--skip-disks', type=int, default=1,
                            help="The number devices, starting from a, to skip because already "
                            "hosting other partitions (like root). For example: 1 means skipping "
                            "/dev/sda, 2 means skipping /dev/sd[a,b], etc..")
        parser.add_argument('--partitions-basedir', type=str, default='/var/lib/hadoop/data',
                            help="The base directory of the partitions to initialize.")
        parser.add_argument('--wipe-partitions', action='store_true',
                            help="Use wipefs to remove any pre-existing partition table on the disks.")
        parser.add_argument('--success-percent', type=int, default=100, choices=range(1, 100),
                            metavar="[1-100]",
                            help="Expected success percent when executing cumin commands on multiple hosts."
                                 "Useful to init old nodes with potentially broken disks.")
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return InitHadoopWorkersRunner(args, self.spicerack)


class InitHadoopWorkersRunner(CookbookRunnerBase):
    """Init Hadoop worker runner."""

    def __init__(self, args, spicerack):
        """Initialize an Hadoop worker."""
        self.success_percent_cumin = args.success_percent / 100
        self.skip_disks = args.skip_disks
        self.disks_number = args.disks_number
        self.hostname_pattern = args.hostname_pattern
        self.partitions_basedir = args.partitions_basedir
        self.wipe_partitions = args.wipe_partitions
        self.hadoop_workers = spicerack.remote().query(self.hostname_pattern)

        letters = list(string.ascii_lowercase)
        if len(letters[self.skip_disks:]) < self.disks_number:
            raise RuntimeError(
                'The number of available letters is not enough to support {} disks, '
                'please check your parameters:\n{}'
                .format(self.disks_number, letters[self.skip_disks:]))

        self.available_disk_labels = letters[
            self.skip_disks:self.disks_number + self.skip_disks]

        ask_confirmation(
            'Please check that the hosts to initialize are the expected ones: {}'
            .format(self.hadoop_workers.hosts))

        ask_confirmation(
            'Please check that the disk labels to act on are the expected '
            'ones: {}'.format(str(self.available_disk_labels)))

        ensure_shell_is_durable()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for hosts {}'.format(self.hadoop_workers.hosts)

    def run(self):
        """Initialize an Hadoop worker."""
        logger.info('Installing parted and megacli.')
        confirm_on_failure(
            self.hadoop_workers.run_async, 'apt-get install -y megacli parted')

        logger.info('Creating ext4 disk partitions.')
        for label in self.available_disk_labels:
            device = '/dev/sd' + label
            mountpoint = self.partitions_basedir + '/' + label
            logger.info('Working on %s', device)
            commands = []
            mkfs_prefix = ''
            if self.wipe_partitions:
                commands += [
                    # Partitions can already be unmounted, this step is only a precaution
                    # to avoid subsequent failures.
                    '/bin/umount ' + mountpoint + ' > /dev/null 2>&1 || /bin/true',
                    # To avoid any automatic partition remount after 'mkpart primary ext4' from
                    # systemd, it is necessary to clean up fstab and reload systemd's config.
                    "sed '/" + mountpoint.replace('/', '\\/') + "/d' /etc/fstab -i",
                    "sed '/Hadoop DataNode partition " + label + "/d' /etc/fstab -i",
                    "sed '/Hadoop datanode " + label + " partition/d' /etc/fstab -i",
                    'systemctl daemon-reload']
                # The mkfs command will ask Y/N to overwrite the partition if another one
                # is already there.
                mkfs_prefix = 'echo Y | '
            commands += [
                '/sbin/parted {} --script mklabel gpt'.format(device),
                '/sbin/parted {} --script mkpart primary ext4 0% 100%'.format(device),
                mkfs_prefix + '/sbin/mkfs.ext4 -L hadoop-' + label + " " + device + '1',
                '/sbin/tune2fs -m 0 ' + device + '1']

            confirm_on_failure(
                self.hadoop_workers.run_async, *commands,
                success_threshold=self.success_percent_cumin)

        logger.info('Configuring mountpoints.')
        for label in self.available_disk_labels:
            mountpoint = self.partitions_basedir + '/' + label
            logger.info('Working on %s', mountpoint)
            confirm_on_failure(
                self.hadoop_workers.run_async,
                '/bin/mkdir -p ' + mountpoint,
                'echo -e "# Hadoop DataNode partition ' + label +
                '\nLABEL=hadoop-' + label + "\t" + mountpoint +
                '\text4\tdefaults,noatime\t0\t2" | tee -a /etc/fstab',
                '/bin/mount -v ' + mountpoint,
                success_threshold=self.success_percent_cumin)

        logger.info('Ensure some MegaCLI specific settings.')
        try:
            confirm_on_failure(
                self.hadoop_workers.run_async,
                # See http://lists.us.dell.com/pipermail/linux-poweredge/2006-May/025738.html for more info.
                # All the explanations described below must be credited to the author of the above forum response.
                #
                # Read Policy:
                # The read policies indicate whether or not the controller should read sequential sectors of the
                # logical drive when seeking data.
                # Adaptive Read-Ahead (ADRA): When using adaptive read-ahead policy, the controller initiates read-ahead
                #     only if the two most recent read requests accessed sequential sectors of the disk. If subsequent
                #     read requests access random sectors of the disk, the controller reverts to no-read-ahead policy.
                #     The controller continues to evaluate whether read requests are accessing sequential sectors of
                #     the disk, and can initiate read-ahead if necessary.
                '/usr/sbin/megacli -LDSetProp ADRA -LALL -aALL',

                # The Direct I/O and Cache I/O cache policies apply to reads on a specific virtual disk.
                # These settings do not affect the read-ahead policy.
                # Direct: Specifies that reads are not buffered in cache memory. When using direct I/O,
                #         data is transferred to the controller cache and the host system simultaneously
                #         during a read request.
                #         If a subsequent read request requires data from the same data block, it can be
                #         read directly from the controller cache.
                #         The direct I/O setting does not override the cache policy settings.
                # Direct (No cache)
                '/usr/sbin/megacli -LDSetProp -Direct -LALL -aALL',

                # Write policy:
                # The write policies specify whether the controller sends a write-request completion signal as soon
                # as the data is in the cache or after it has been written to disk.
                # Write-Back. When using write-back caching, the controller sends a write-request
                #     completion signal as soon as the data is in the controller cache
                #     but has not yet been written to disk.
                # Write-Through. When using write-through caching, the controller sends
                #     a write-request completion signal only after the data is written to the disk.
                #
                # Set no write cache if bad BBU (default is WriteBack)
                '/usr/sbin/megacli -LDSetProp NoCachedBadBBU -LALL -aALL',

                # Disable BBU auto-learn
                'echo "autoLearnMode=1" > /tmp/disable_learn',
                '/usr/sbin/megacli -AdpBbuCmd -SetBbuProperties -f /tmp/disable_learn -a0',
                success_threshold=self.success_percent_cumin)
        except RemoteExecutionError:
            logger.error('An error has occurred while setting MegaCLI options.')
            ask_confirmation('Please check logs and continue (if it is the case).')

        return 0
