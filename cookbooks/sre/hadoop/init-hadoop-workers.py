"""Initialize a new Hadoop worker

This cookbook helps the Analytics team in setting up new Hadoop
workers, configuring the Hadoop disk partitions not currently
handled by partman during d-i.
"""
import argparse
import logging
import string

from spicerack.interactive import ask_confirmation, ensure_shell_is_durable
from cookbooks import ArgparseFormatter

__title__ = 'Initialize a new Hadoop worker'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """Argument parser helper function"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
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
    return parser


def run(args, spicerack):
    """Initialize an Hadoop worker"""
    ensure_shell_is_durable()

    available_disk_labels = list(string.ascii_lowercase)[args.skip_disks:args.disks_number + 1]
    hadoop_workers = spicerack.remote().query(args.hostname_pattern)

    ask_confirmation(
        'Please check that the hosts init are correct: {}'
        .format(hadoop_workers.hosts))

    logger.info('Installing parted and megacli.')
    hadoop_workers.run_async('apt-get install -y megacli parted')

    logger.info('Creating ext4 disk partitions.')
    for label in available_disk_labels:
        device = '/dev/sd' + label
        hadoop_workers.run_async([
            '/sbin/parted {} --script mklabel gpt'.format(device),
            '/sbin/parted {} --script mkpart primary ext4 0% 100%'
            .format(device),
            '/sbin/mkfs.ext4 -L hadoop-' + label + " " + device + '1',
            '/sbin/tune2fs ' + device + '1',
        ])

    logger.info('Configuring mountpoints.')
    for label in available_disk_labels:
        mountpoint = args.partitions_basedir + '/' + label
        hadoop_workers.run_async([
            '/bin/mkdir -p ' + mountpoint,
            'echo -e "# Hadoop DataNode partition ' + label +
            '\nLABEL=hadoop-' + label + "\t" + mountpoint + '\text4\tdefaults,noatime\t0\t2" | tee -a /etc/fstab',
            '/bin/mount -v ' + mountpoint
        ])

    logger.info('Ensure some MegaCLI specific settings.')
    hadoop_workers.run_async([
        # ReadAhead Adaptive
        '/usr/sbin/megacli -LDSetProp ADRA -LALL -aALL',
        # Direct (No cache)
        '/usr/sbin/megacli -LDSetProp -Direct -LALL -aALL',
        # No write cache if bad BBU
        '/usr/sbin/megacli -LDSetProp NoCachedBadBBU -LALL -aALL',
        # Disable BBU auto-learn
        'echo "autoLearnMode=1" > /tmp/disable_learn',
        '/usr/sbin/megacli -AdpBbuCmd -SetBbuProperties -f /tmp/disable_learn -a0'
    ])
