# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
This module implements the swift remote_log backend which handle sending of
OSArchiver log files into swift backend.
This module use the high level module SwiftService
"""

import logging
from os.path import basename
from swiftclient.service import SwiftError, SwiftService, SwiftUploadObject

from osarchiver.destination.file.remote_store import RemoteStore


class Swift(RemoteStore):
    """
    Swift class used to send log remotely on openstack's swift backend
    """

    def __init__(self, name=None, date=None, store_options={}):
        """
        instance osarchiver.remote_log.swift class
        """
        RemoteStore.__init__(self, backend='swift', name=name,
                             date=date, store_options=store_options)
        self.container = store_options.get('container', None)
        self.file_name_prefix = store_options.get('file_name_prefix', '')
        self.service = None

    def send(self, files=[]):
        """
        send method implemented which is in charge of sending local log files
        to a remote swift destination.
        """

        options = self.store_options
        with SwiftService(options=options) as swift:
            file_objects = [
                SwiftUploadObject(f,
                                  object_name='%s/%s/%s' % (
                                      self.file_name_prefix,
                                      self.date,
                                      basename(f))
                                  ) for f in files]
            for r in swift.upload(self.container, file_objects):
                if r['success']:
                    if 'object' in r:
                        logging.info("%s successfully uploaded" % r['object'])
                else:
                    error = r['error']
                    if r['action'] == "create_container":
                        logging.error("Failed to create container %s: %s",
                                      self.container, error)
                    elif r['action'] == "upload_object":
                        logging.error("Failed to upload file %s: %s",
                                      r['object'], error)
                    else:
                        logging.error("Unknown error while uploading file: %s",
                                      error)

    def clean_exit(self):
        """
        Tasks to be executed to exit cleanly
        """
        pass


if __name__ == '__main__':
    pass
