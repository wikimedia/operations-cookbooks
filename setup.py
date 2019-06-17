"""Package configuration."""

from setuptools import find_packages, setup

install_requires = [
    'python-dateutil',
    'wikimedia-spicerack',
]

# Extra dependencies
extras_require = {
    # Test dependencies
    'tests': [
        'bandit>=1.1.0',
        'flake8>=3.2.1',
        'prospector[with_everything]>=0.12.4,<=1.1.6.2',
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
