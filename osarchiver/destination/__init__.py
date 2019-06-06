# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
init file that allow to load osarchiver.source whithout loading submodules
"""

from osarchiver.common import backend_factory
from osarchiver.destination.base import Destination


def factory(*args, backend='db', **kwargs):
    """
    backend factory
    """
    return backend_factory(*args,
                           backend=backend,
                           module='osarchiver.destination',
                           subclass=Destination,
                           **kwargs)
