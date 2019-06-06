# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.

"""
Common functions of osarchiver
"""
from importlib import import_module


def backend_factory(*args, backend='db', module=None, subclass=None, **kwargs):
    """
    This factory function rule is to return the backend instances
    It raises an exception on import or attribute error or unavailable backend
    """
    try:
        class_name = backend.capitalize()
        backend_module = import_module(module + '.' + backend)
        backend_class = getattr(backend_module, class_name)
        instance = backend_class(*args, **kwargs)
    except (AttributeError, ImportError):
        raise ImportError("{} is not part of our backend"
                          " collection!".format(backend))
    else:
        if not issubclass(backend_class, subclass):
            raise ImportError("Unsupported '{}' destination"
                              " backend".format(backend))
    return instance
