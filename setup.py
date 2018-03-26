#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='bdk',
    version='0.0.13',
    description='Task manager',
    author='Yandex-load team',
    author_email='load@yandex-team.ru',
    url='https://github.com/yandex-load/bdk',
    packages=find_packages(exclude=["tests", "tmp", "docs", "data"]),
    install_requires=[
        'requests>=2.11.1',
        'simplejson',
        'netort>=0.0.14',
        'pyyaml', 'retrying'
    ],
    entry_points={
        'console_scripts': [
            'bdk = bdk.api.cli:main',
        ],
    },
    package_data={
        'bdk.core': ['config/*'],
    },
)
