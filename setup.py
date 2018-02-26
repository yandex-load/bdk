#!/usr/bin/env python

from setuptools import setup

setup(
    name='bdk',
    version='0.0.3',
    description='Task manager',
    author='Yandex load team',
    author_email='load@yandex-team.ru',
    url='https://github.yandex-team.ru/load/bdk/',
    packages=['bdk'],
    package_data={'bdk': []},
    install_requires=[
        'requests>=2.11.1',
        'simplejson',
        'netort>=0.0.6'
    ],
    entry_points={
        'console_scripts': [
            'bdk = bdk.api.cli:main',
        ],
    },)
