# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='bq',
    version='0.0.1',
    author='Yuuki Furuyama',
    author_email='addsict@gmail.com',
    url='https://github.com/addsict/bq',
    download_url='https://github.com/addsict/bq/tarball/master',
    description='BigQuery library with Google App Engine',
    long_description='',
    platform='any',
    keywords='bigquery google app engine',
    licenst='MIT',
    py_modules=[
        'bq',
        ],
    install_requires=[
        'bigquery>=2.0.12',
        ],
    )
