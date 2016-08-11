#!/usr/bin/env python

from setuptools import setup

setup(
    name='bdk',
    version='0.0.1',
    description='Tank manager',
    author='Alexey Lavrenuke',
    author_email='direvius@gmail.com',
    url='https://github.yandex-team.ru/load/bdk/',
    packages=['bdk'],
    package_data={'bdk': []},
    install_requires=[
        'requests',
    ],
    entry_points={
        'console_scripts': [
            'bdk = bdk.service:main',
        ],
    },)
