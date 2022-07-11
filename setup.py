"""Package configuration."""

from setuptools import find_packages, setup

# The below list is only for CI
# For prod add the libs to modules/profile/manifests/spicerack.pp
install_requires = [
    'prettytable',
    # Force urllib3 and pyyaml version as required by {'elasticsearch-curator'} from spicerack
    # As of 2022-07-12,
    # On Ubuntu Jammy, (setuptools 59.6.0, pip 22.0.2, python 3.10.4) this is required.
    # On Debian Bookworm, (setuptools 59.6.0, pip 22.1.1, python 3.10.5) this is not needed.
    # Remove once upstream conflict is resolved
    'urllib3==1.26.4',
    'pyyaml==5.4.1',
    'python-dateutil',
    'wikimedia-spicerack',
    # [fixme]: The dnspython requirement is not reflected in puppet and should be
    # removed when moving generic functions from sre.discovery.service-route to
    # spicerack. 1.16.0 is currently in buster and 2.0.0 changed a lot, so I'm
    # pinning this here to have the right version with local tox.
    'dnspython==1.16.0',
    'defusedxml',
]

# Extra dependencies
extras_require = {
    # Test dependencies
    'tests': [
        'bandit>=1.5.0',
        'flake8>=3.2.1',
        'pytest>=6.1.0',
    ],
    'prospector': [
        'prospector[with_everything]>=0.12.4,<=1.7.7',
        'pylint<2.15.7',  # Temporary upper limit for an upstream regression
        'pytest>=6.1.0',
    ],
}

setup_requires = [
    'setuptools_scm>=1.15.0',
]

setup(
    author='Riccardo Coccioli',
    author_email='rcoccioli@wikimedia.org',
    description='Wikimedia Foundations production automation and orchestration cookbooks',
    extras_require=extras_require,
    install_requires=install_requires,
    keywords=['wmf', 'automation', 'orchestration', 'cookbooks'],
    license='GPLv3+',
    name='wikimedia-cookbooks',
    packages=find_packages(exclude=['*.tests', '*.tests.*']),
    platforms=['GNU/Linux'],
    setup_requires=setup_requires,
    use_scm_version=True,
    url='https://github.com/wikimedia/operations-cookbooks',
    zip_safe=False,
)
