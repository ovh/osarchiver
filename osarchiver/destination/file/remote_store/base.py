# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.

"""
RemoteStore abstract base class file
"""

from abc import ABCMeta, abstractmethod
import arrow
import re


class RemoteStore(metaclass=ABCMeta):
    """
    The RemoteStore abstract class
    """

    def __init__(self, name=None, backend='swift', date=None, store_options={}):
        """
        RemoteStore object is defined by a name and a backend
        """
        self.name = name
        self.date = date or arrow.now().strftime('%F_%T')
        self.backend = backend
        self.store_options = {
            re.sub('^opt_', '', k): v for k, v in store_options.items() if k.startswith('opt_')
        }

    @abstractmethod
    def send(self, files=[]):
        """
        Send method that should be implemented by the backend
        """
