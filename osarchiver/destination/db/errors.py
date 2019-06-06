# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
Destination Db implementation exceptions
"""

from osarchiver.errors import OSArchiverException


class OSArchiverNotEqualDbCreateStatements(OSArchiverException):
    """
    Exception raised when create statement is different between source and
    destination database
    """

    def __init__(self, message=None):
        super().__init__(message='The CREATE DATABASE statement is not equal '
                         'between src and dst')


class OSArchiverNotEqualTableCreateStatements(OSArchiverException):
    """
    Exception raised when create statement is different between source and
    destination table
    """

    def __init__(self, message=None):
        super().__init__(message='The SHOW CREATE TABLE statement is not equal'
                         ' between src and dst table')
