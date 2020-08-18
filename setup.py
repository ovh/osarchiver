#!/usr/bin/env python3

import os
import re

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

setup(name='Openstack DB archiver',
      version='0.1.0',
      description="Openstack DB archiver",
      long_description=open('README.md').read(),
      author='OVH SAS',
      author_email='opensource@ovh.net',
      license='Apache 2.0',
      url="https://github.com/ovh/osarchiver",
      classifiers=[
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.2',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Topic :: Software Development :: Libraries :: Python Modules'
      ],
      packages=find_packages(".", exclude=('tests')),
      entry_points={'console_scripts': ['osarchiver=osarchiver.main:run']})
