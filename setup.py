#!/usr/bin/env python3

import os
import re

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

# Determine version from changelog file
VERSION = None
if os.path.exists('debian/changelog'):
    for line in open('debian/changelog').readlines():
        m = re.search(r'\((.*)-(.*)\)', line)
        if m:
            version = m.group(1)
        if version:
            break

setup(name='Openstack Archiver',
      version=version,
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
      install_requires=[
          'python-dateutil', 'arrow', 'configparser', 'PyMySQL', 'numpy'
      ],
      entry_points={'console_scripts': ['osarchiver=osarchiver.main:run']})
