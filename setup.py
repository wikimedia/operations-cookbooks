"""Package configuration."""

from setuptools import find_namespace_packages, setup

# The below list is only for CI
# For prod add the libs to modules/profile/manifests/spicerack.pp
install_requires = [
    'prettytable',
    'python-dateutil',
    'python-gitlab==3.11.0',
    'wikimedia-spicerack',
    # on cumin nodes transferpy v1.1 is installed as the Debian package
    'transferpy @ git+https://gerrit.wikimedia.org/r/operations/software/transferpy@v1.1',
]

# Extra dependencies
extras_require = {
    # Test dependencies
    'tests': [
        'bandit>=1.5.0',
        'flake8>=3.2.1',
        'mypy>=0.670',
        'pytest>=6.1.0',
        'types-PyMySQL',
        'types-python-dateutil',
        'types-PyYAML',
        'types-redis',
        'types-requests',
        'types-setuptools',
    ],
    'prospector': [
        'prospector[with_everything]>=0.12.4',
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
    packages=find_namespace_packages(include=['cookbooks'], exclude=['*.tests', '*.tests.*']),
    platforms=['GNU/Linux'],
    setup_requires=setup_requires,
    use_scm_version=True,
    url='https://github.com/wikimedia/operations-cookbooks',
    zip_safe=False,
)
