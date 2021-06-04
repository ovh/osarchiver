#!/usr/bin/env python3

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

setup(name='osarchiver',
      version='0.2.0',
      license='Apache 2.0',
      setup_requires=['pbr>=2.0.0'],
      pbr=True)
