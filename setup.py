#!/usr/bin/env python3

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

setup(setup_requires=['pbr>=2.0.0'],
      pbr=True)
