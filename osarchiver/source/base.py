# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.

"""
Source abstract base class file
"""

from abc import ABCMeta, abstractmethod


class Source(metaclass=ABCMeta):
    """
    The source absrtact base class
    """
    def __init__(self, name=None, backend=None):
        """
        Source object is defined by a name and a backend
        """
        self.name = name
        self.backend = backend

    @abstractmethod
    def read(self, **kwargs):
        """
        read method that should be implemented by the backend
        """

    @abstractmethod
    def delete(self, **kwargs):
        """
        delete method that should be implemented by the backend
        """

    @abstractmethod
    def clean_exit(self):
        """
        clean_exit method that should be implemented by the backend
        provide a way to close and clean properly backend stuff
        """
