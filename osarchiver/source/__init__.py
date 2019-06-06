# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.

"""
init file that allow to import Source from osarchiver.source whithout loading
submodules.
"""


from osarchiver.common import backend_factory
from osarchiver.source.base import Source


def factory(*args, backend='db', **kwargs):
    """
    backend factory
    """
    return backend_factory(*args,
                           backend=backend,
                           module='osarchiver.source',
                           subclass=Source,
                           **kwargs)
