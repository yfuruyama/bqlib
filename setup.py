# -*- coding: utf-8 -*-
from setuptools import setup


setup(
    name='bqlib',
    version='0.0.2',
    description='BigQuery Python Library',
    long_description=open('README.txt').read(),
    author='Yuuki Furuyama',
    author_email='addsict@gmail.com',
    url='https://github.com/addsict/bqlib',
    download_url='https://github.com/addsict/bqlib/tarball/master',
    keywords='google bigquery python library bqlib query',
    license='MIT',
    py_modules=[
        'bqlib',
        ],
    install_requires=[
        'bigquery>=2.0.17',
        ],
    )
