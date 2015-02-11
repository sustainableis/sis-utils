#!/usr/bin/python

from os import path

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

README = path.abspath(path.join(path.dirname(__file__), 'README.md'))
desc = 'A Python library for SIS resources to use for various common tasks'

setup(
  name='sis-utils',
  version='0.2.5',
  description=desc,
  long_description=open(README).read(),
  package_dir={'sisutils': 'sisutils','sisutils.config':'sisutils/config','sisutils.mq':'sisutils/mq','sisutils.endpoint':'sisutils/endpoint','sisutils.email':'sisutils/email','sisutils.sftp':'sisutils/sftp'},
  packages=['sisutils','sisutils.mq','sisutils.config','sisutils.endpoint','sisutils.email','sisutils.sftp'],
  install_requires=['msgpack-python','mandrill','psycopg2','mysql-connector-python','paramiko'],
  author='John Crawford',
  author_email='jcrawford@sustainableis.com',
  url='https://github.com/sustainableis/sis-utils',
  download_url='https://github.com/sustainableis/sis-utils',
  license='None',
  classifiers=[
    'Programming Language :: Python :: 2',
    'Intended Audience :: Developers',
  ],
)
