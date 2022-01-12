# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.

"""
The Archiver class file
"""

import logging
import traceback
from osarchiver.errors import OSArchiverArchivingFailed


class Archiver():
    """
    Archiver class
    """

    def __init__(self, name=None, src=None, dst=None, conf=None):
        """
        instantiator, take one source and a list of destinations
        """
        self.name = name
        # One source
        self.src = src
        # Pool of destinations
        self.dst = dst or []
        # Config parser instance
        self.conf = conf

    def __repr__(self):
        return "Archiver {name}: {src} -> {dst}".\
            format(name=self.name, src=self.src, dst=self.dst)

    def read(self):
        """
        read method which loop over each set of data from Source instance
        yield database, table, items
        """
        for data in self.src.read():
            for items in data['data']:
                yield (data['database'], data['table'], items)

    def write(self, database=None, table=None, data=None):
        """
        write method take a set of data as arguments, database and table and
        loop over each destination configured and call the write method of the
        destination object raise OSArchiverArchivingFailed in case of archiving
        error to prevent deletion
        """
        if not self.src.archive_data:
            logging.info("Ignoring data archiving because archive_data is "
                         "set to %s", self.src.archive_data)
        else:
            for dst in self.dst:
                try:
                    dst.write(database=database, table=table, data=data)
                except Exception as my_exception:
                    logging.error(
                        "An error occured while archiving data: %s",
                        my_exception)
                    logging.error("Full traceback is: %s",
                                  traceback.format_exc())
                    raise OSArchiverArchivingFailed

    def delete(self, database=None, table=None, data=None):
        """
        delete method take a set of data, database, table as arguments and
        delete the data from source if the delete_data prameters is true
        """
        if not self.src.delete_data:
            logging.debug("Ignoring data deletion because delete_data is "
                          "set to %s", self.src.delete_data)
        else:
            try:
                self.src.delete(database=database, table=table, data=data)
            except Exception as my_exception:
                logging.error("An error occured while deleting data: %s",
                              my_exception)
                logging.error("Full traceback is: %s", traceback.format_exc())

    def run(self):
        """
        main method which basically read a set of data from the source then
        archive the data and delete them if no exception were caught
        """
        if not self.src.archive_data and not self.src.delete_data:
            logging.info("Nothing to do for archiver %s archive_data and "
                         "delete_date are disabled", self.name)
            return 0

        if not self.src.delete_data:
            logging.info("Data won't be deleted because 'delete_data' set to"
                         " %s", self.src.delete_data)

        for (database, table, items) in self.read():
            try:
                self.write(database=database, table=table, data=items)
            except OSArchiverArchivingFailed:
                logging.info("Ignoring deletion step because an error occured "
                             "while archiving data")
            else:
                self.delete(database=database, table=table, data=items)

        self.clean_exit()
        return 0

    def clean_exit(self):
        """
        method called when archiving is finished. It calls clean_exit method of
        Source and Destination instances
        """
        logging.info("Please wait for clean exit...")
        self.src.clean_exit()
        for dst in self.dst:
            dst.clean_exit()
