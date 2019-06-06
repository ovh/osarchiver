# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.

"""
Implementation of CSV writer (SQL data -> SQL file)
"""

import logging
import re
import pymysql
from osarchiver.destination.file.base import Formatter


class Sql(Formatter):
    """
    The class implement a formatter of SQL type which is able to convert a list
    of dict of data into one file of SQL statement
    """

    def get_handler(self, handler=None, file_to_handle=None):
        """
        Return a file handler if it already exists or create a new one
        """
        if handler not in self.handlers:
            self.handlers[handler] = {}
            self.handlers[handler]['file'] = file_to_handle
            self.handlers[handler]['fh'] = open(file_to_handle,
                                                'w',
                                                encoding='utf-8')

        return self.handlers[handler]['fh']

    def write(self, database=None, table=None, data=None):
        """
        The write method which should be implemented because of ineherited
        Formatter class
        The name of the file is of the form <database>.<table>.sql
        The SQL statement is:
            INSERT INTO <database>.<table> (col1, col2, ... ) VALUES
            (val1, val2, ... )
            ON DUPLICATE KEY UPDATE <primary_key>=<primary_key>
        This will help in importing again a file without removing already
        inserted lines
        """
        destination_file = '{directory}/{db}.{table}.sql'.format(
            directory=self.directory, db=database, table=table)
        key = '{db}.{table}'.format(db=database, table=table)

        writer = self.get_handler(handler=key, file_to_handle=destination_file)
        lines = []
        primary_key = self.source.get_table_primary_key(database=database,
                                                        table=table)
        for item in data:
            # Build columns insert part
            # iterate over keys or values of dict is consitent in python 3
            columns = '`' + '`, `'.join(item.keys()) + '`'
            # SQL scaping, None is changed to NULL
            values = [
                pymysql.escape_string(str(v)) if v is not None else 'NULL'
                for v in item.values()
            ]
            placeholders = "'" + "', '".join(values) + "'"
            # Remove the simple quote around NULL statement to be understood as
            # a MysQL NULL key word.
            placeholders = re.sub("'NULL'", "NULL", placeholders)
            # The SQL statement
            sql = "INSERT INTO {database}.{table} ({columns}) VALUES "\
                "({placeholders}) ON DUPLICATE KEY UPDATE {pk} = {pk};\n".\
                format(
                    database=database,
                    table=table,
                    columns=columns,
                    placeholders=placeholders,
                    pk=primary_key
                )
            lines.append(sql)

        logging.info("%s formatter: writing %s lines in %s", self.name,
                     len(data), destination_file)
        if not self.dry_run:
            writer.writelines(lines)
        else:
            logging.debug("[DRY RUN] No data writen in %s", destination_file)
