# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.

"""
Implementation of CSV writer (SQL data -> CSV file)
"""

import logging
import csv
from osarchiver.destination.file.base import Formatter


class Csv(Formatter):
    """
    The class implement a formatter of CSV type which is able to convert a list
    of dict of SQL data into one CSV file
    """

    def write(self, database=None, table=None, data=None):
        """
        The write method which should be implemented because of ineherited
        Formatter class.
        The name of the file is of the form <database>.<table>.csv
        """

        destination_file = '{directory}/{db}.{table}.csv'.format(
            directory=self.directory, db=database, table=table,
        )
        key = '{db}.{table}'.format(db=database, table=table)

        writer = None
        if key in self.handlers:
            writer = self.handlers[key]['csv_writer']
        else:
            self.handlers[key] = {}
            self.handlers[key]['file'] = destination_file
            self.handlers[key]['fh'] = open(
                destination_file, 'w', encoding='utf-8')
            self.handlers[key]['csv_writer'] = \
                csv.DictWriter(
                    self.handlers[key]['fh'],
                    fieldnames=[h for h in data[0].keys()])
            writer = self.handlers[key]['csv_writer']
            if not self.dry_run:
                logging.debug("It seems this is the first write set, adding "
                              " headers to CSV file")
                writer.writeheader()
            else:
                logging.debug(
                    "[DRY RUN] headers not written in %s", destination_file)

        logging.info("%s formatter: writing %s line in %s", self.name,
                     len(data), destination_file)
        if not self.dry_run:
            writer.writerows(data)
        else:
            logging.debug("[DRY RUN] No data written in %s", destination_file)
