# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='bqlib',
    version='0.0.1',
    author='Yuuki Furuyama',
    author_email='addsict@gmail.com',
    url='https://github.com/addsict/bqlib',
    download_url='https://github.com/addsict/bqlib/tarball/master',
    description='BigQuery python library',
    long_description='',
    platform='any',
    keywords='google bigquery python library',
    licenst='MIT',
    py_modules=[
        'bqlib',
        ],
    install_requires=[
        'bigquery>=2.0.12',
        ],
    )
