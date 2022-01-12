# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.

"""
Destination abstract base class file
"""

from abc import ABCMeta, abstractmethod


class Destination(metaclass=ABCMeta):
    """
    The Destination absrtact base class
    """

    def __init__(self, name=None, backend='db', conf=None):
        """
        Destination object is defined by a name and a backend
        """
        self.name = name
        self.backend = backend
        self.conf = conf

    @abstractmethod
    def write(self, database=None, table=None, data=None):
        """
        Write method that should be implemented by the backend
        """

    @abstractmethod
    def clean_exit(self):
        """
        clean_exit method that should be implemented by the backend
        provide a way to close and clean properly backend stuff
        """
