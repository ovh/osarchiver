# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
OSArchiver exceptions base class
"""


class OSArchiverException(Exception):
    """
    OSArchiver base exception class
    """
    def __init__(self, message=None):
        """
        Instance the exception base class
        """
        super().__init__(message)
        self.message = message

    def __str__(self):
        return self.message


class OSArchiverArchivingFailed(OSArchiverException):
    """
    Exception raised when archiving fail
    """
    def __init__(self, message=None):
        super().__init__(message='Archiving of data set failed')
