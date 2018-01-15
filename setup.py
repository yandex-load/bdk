#!/usr/bin/env python

from setuptools import setup

setup(
    name='bdk',
    version='0.0.2',
    description='Tank manager',
    author='Alexey Lavrenuke',
    author_email='direvius@gmail.com',
    url='https://github.yandex-team.ru/load/bdk/',
    packages=['bdk'],
    package_data={'bdk': []},
    install_requires=[
        'requests>=2.11.1', 'simplejson'
    ],
    entry_points={
        'console_scripts': [
            'bdk = bdk.service:main',
        ],
    },)
