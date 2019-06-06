# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
This module implements the database destination backend which handle writing
data into a MySQL/MariaDB backend
"""

import logging
import difflib
import re
import time
import arrow
from osarchiver.destination import Destination
from osarchiver.common.db import DbBase
from . import errors as db_errors


class Db(Destination, DbBase):
    """
    Db class which is instanced when Db backend is required
    """

    def __init__(self,
                 table=None,
                 archive_data=None,
                 name=None,
                 source=None,
                 db_suffix='',
                 table_suffix='',
                 database=None,
                 **kwargs):
        """
        instance osarchiver.destination.Db class backend
        """
        self.database = database
        self.table = table
        self.archive_data = archive_data
        self.source = source
        self.archive_db_name = None
        self.archive_table_name = None
        self.table_db_name = None
        self.db_suffix = db_suffix
        self.table_suffix = table_suffix
        self.normalized_db_suffixes = {}
        Destination.__init__(self, backend='db', name=name)
        DbBase.__init__(self, **kwargs)

    def __repr__(self):
        return "Destination {name} [Backend:{backend} - Host:{host}]".format(
            backend=self.backend, host=self.host, name=self.name)

    def normalize_db_suffix(self, db_suffix='', database=None):
        """
        Return the name of the suffix that should be added to database name to
        build the archive database name in which archive data. It checks that
        it is not archived in the same Db than Source.
        The database name may contains '{date}' which will be replaced by the
        date of archiving in the format '2019-01-17_10:42:42'
        """

        if database is not None and database in self.normalized_db_suffixes:
            logging.debug("Using cached db suffix '%s' of '%s' database",
                          self.normalized_db_suffixes[database], database)
            return self.normalized_db_suffixes[database]

        if db_suffix:
            self.db_suffix = db_suffix

        # in case source and destination are the same
        # archiving in the same db is a non sense
        # force db suffix to _'archive in that case
        # unless table_suffix is set
        if self.source.host == self.host and  \
                self.source.port == self.port and \
                not self.db_suffix:
            self.db_suffix = '_archive'
            logging.warning(
                "Your destination host is the same as the source "
                "host, to prevent writing on the same database, "
                "which could result in data loss the suffix of DB "
                "is forced to %s", self.db_suffix)

        if self.source.host == self.host and \
                self.source.port != self.port and \
                not self.db_suffix and not self.table_suffix:
            logging.warning("!!!! I can't verify that destination database is "
                            "different of source database, you may loose data,"
                            " BE CAREFULL!!!")
            logging.warning("Sleeping 10 sec...")
            time.sleep(10)

        self.db_suffix = str(
            self.db_suffix).format(date=arrow.now().strftime('%F_%T'))

        if database is not None:
            self.normalized_db_suffixes[database] = self.db_suffix
            logging.debug("Caching db suffix '%s' of '%s' database",
                          self.normalized_db_suffixes[database], database)

        return self.db_suffix

    def normalize_table_suffix(self, table_suffix=None):
        """
        Return the suffix of table in which archive data.
        The table name may contains '{date}' which will be replaced by the date
        of archiving in the format '2019-01-17_10:42:42'
        """
        if table_suffix:
            self.table_suffix = table_suffix

        self.table_suffix = str(
            self.table_suffix).format(date=arrow.now().strftime('%F_%T'))

        return self.table_suffix

    def get_archive_db_name(self, database=None):
        """
        Return the name of the archiving database, which is build from the name
        of the source database plus a suffix
        """
        self.archive_db_name = database + \
            self.normalize_db_suffix(database=database)
        return self.archive_db_name

    def archive_db_exists(self, database=None):
        """
        Check if a databae already exists, return True/False
        """
        self.get_archive_db_name(database=database)
        show_db_sql = "SHOW DATABASES LIKE "\
            "'{db}'".format(db=self.archive_db_name)
        return bool(self.db_request(sql=show_db_sql, fetch_method='fetchall'))

    def get_src_create_db_statement(self, database=None):
        """
        Return result of SHOW CREATE DATABASE of the Source
        """
        src_db_create_sql = "SHOW CREATE DATABASE "\
            "{db}".format(db=database)
        src_db_create_statement = self.source.db_request(
            sql=src_db_create_sql, fetch_method='fetchone')[1]
        logging.debug("Source database '%s' CREATE statement: '%s'", database,
                      src_db_create_statement)
        return src_db_create_statement

    def get_dst_create_db_statement(self, database=None):
        """
        Return result of SHOW CREATE DATABASE of the Destination
        """
        dst_db_create_sql = "SHOW CREATE DATABASE "\
            "{db}".format(db=database)
        dst_db_create_statement = self.db_request(sql=dst_db_create_sql,
                                                  fetch_method='fetchone')[1]
        logging.debug("Destination database '%s' CREATE statement: '%s'",
                      database, dst_db_create_statement)
        return dst_db_create_statement

    def create_archive_db(self, database=None):
        """
        Create the Destination database
        It checks that if the Destination database exists, the show create
        statement are the same than Source which is useful to detect Db schema
        upgrade
        """
        # Check if db exists
        archive_db_exists = self.archive_db_exists(database=database)

        # retrieve source db create statement
        # if archive database exists, compare create statement
        # else use the statement to create it
        src_db_create_statement = self.get_src_create_db_statement(
            database=database)

        if archive_db_exists:
            logging.debug("Destination DB has '%s' database",
                          self.archive_db_name)
            dst_db_create_statement = self.get_dst_create_db_statement(
                database=self.archive_db_name)

            # compare create statement substituing db name in dst (arbitrary
            # choice)
            to_compare_dst_db_create_statement = re.sub(
                'DATABASE `{dst_db}`'.format(dst_db=self.archive_db_name),
                'DATABASE `{src_db}`'.format(src_db=database),
                dst_db_create_statement)
            if src_db_create_statement == to_compare_dst_db_create_statement:
                logging.info("source and destination database are identical")
            else:
                logging.debug(
                    difflib.SequenceMatcher(
                        None, src_db_create_statement,
                        to_compare_dst_db_create_statement))
                raise db_errors.OSArchiverNotEqualDbCreateStatements

        else:
            logging.debug("'%s' on remote DB does not exists",
                          self.archive_db_name)
            sql = re.sub('`{db}`'.format(db=database),
                         '`{db}`'.format(db=self.archive_db_name),
                         src_db_create_statement)
            self.db_request(sql=sql)
            if not self.dry_run:
                logging.debug("Successfully created '%s'",
                              self.archive_db_name)

    def archive_table_exists(self, database=None, table=None):
        """
        Check if the archiving tabel exists, return True or False
        """
        self.archive_table_name = table + self.normalize_table_suffix()
        show_table_sql = 'SHOW TABLES LIKE '\
            '\'{table}\''.format(table=self.archive_table_name)
        return bool(
            self.db_request(sql=show_table_sql,
                            fetch_method='fetchall',
                            database=self.archive_db_name))

    def get_src_create_table_statement(self, database=None, table=None):
        """
        Return the SHOW CREATE TABLE of Source database
        """
        src_table_create_sql = 'SHOW CREATE TABLE '\
            '{table}'.format(table=table)
        src_table_create_statement = self.source.db_request(
            sql=src_table_create_sql,
            fetch_method='fetchone',
            database=database)[1]
        logging.debug("Source table '%s' CREATE statement: '%s'", database,
                      src_table_create_statement)
        return src_table_create_statement

    def get_dst_create_table_statement(self, database=None, table=None):
        """
        Return the SHOW CREATE TABLE of Destination database
        """
        dst_table_create_sql = 'SHOW CREATE TABLE '\
            '{table}'.format(table=table)
        dst_table_create_statement = self.db_request(sql=dst_table_create_sql,
                                                     fetch_method='fetchone',
                                                     database=database)[1]
        logging.debug("Destination table '%s' CREATE statement: '%s'",
                      self.archive_db_name, dst_table_create_statement)
        return dst_table_create_statement

    def compare_src_and_dst_create_table_statement(self,
                                                   src_statement=None,
                                                   dst_statement=None,
                                                   src_table=None,
                                                   dst_table=None):
        """
        Check that Source and Destination table are identical to prevent errors
        due to db schema upgrade
        It raises an exception if there is a difference and display the
        difference
        """
        # compare create statement substituing db name in dst (arbitrary
        # choice)
        dst_statement = re.sub(
            'TABLE `{dst_table}`'.format(dst_table=dst_table),
            'TABLE `{src_table}`'.format(src_table=src_table), dst_statement)

        # Remove autoincrement statement
        dst_statement = re.sub(r'AUTO_INCREMENT=\d+ ', '', dst_statement)
        src_statement = re.sub(r'AUTO_INCREMENT=\d+ ', '', src_statement)

        logging.debug("Comparing source create statement %s", src_statement)
        logging.debug("Comparing dest create statement %s", dst_statement)

        if dst_statement == src_statement:
            logging.info("source and destination tables are identical")
        else:
            for diff in difflib.context_diff(src_statement.split('\n'),
                                             dst_statement.split('\n')):
                logging.debug(diff.strip())

            raise db_errors.OSArchiverNotEqualTableCreateStatements

    def create_archive_table(self, database=None, table=None):
        """
        Create the archive table in the archive database.
        It checks that Source and Destination table are the identical.
        """
        # Call create db if archive_db_name is None
        if self.archive_db_name is None:
            self.create_archive_db(database=database)
        else:
            logging.debug("Archive db is '%s'", self.archive_db_name)

        # Check if table exists
        archive_table_exists = False
        if self.archive_db_exists:
            archive_table_exists = self.archive_table_exists(database=database,
                                                             table=table)

        # retrieve source tabe create statement
        # if archive table exists, compare create statement
        # else use the statement to create it
        src_create_table_statement = self.get_src_create_table_statement(
            database=database, table=table)

        if archive_table_exists:
            logging.debug("Remote DB has '%s.%s' table", self.archive_db_name,
                          self.archive_table_name)
            dst_table_create_statement = self.get_dst_create_table_statement(
                database=self.archive_db_name, table=self.archive_table_name)
            self.compare_src_and_dst_create_table_statement(
                src_statement=src_create_table_statement,
                dst_statement=dst_table_create_statement,
                src_table=table,
                dst_table=self.archive_table_name)
        else:
            logging.debug("'%s' table on remote DB does not exists",
                          self.archive_table_name)
            sql = re.sub(
                'TABLE `{table}`'.format(table=table),
                'TABLE `{table}`'.format(table=self.archive_table_name),
                src_create_table_statement)
            self.db_request(sql=sql,
                            database=self.archive_db_name,
                            foreign_key_check=False)

            if not self.dry_run:
                logging.debug("Successfully created '%s.%s'",
                              self.archive_db_name, self.archive_table_name)

    def prerequisites(self, database=None, table=None):
        """
        Check that destination database and tables exists before proceeding to
        archiving. Keep the result in metadata for performance purpose.
        """
        if database in self.metadata and table in self.metadata[database]:
            logging.debug("Use cached prerequisites metadata")
            return

        self.metadata[database] = {}
        logging.info("Checking prerequisites")

        self.create_archive_db(database=database)
        self.create_archive_table(database=database, table=table)
        self.metadata[database][table] = \
            {'checked': True,
             'primary_key': self.get_table_primary_key(database=database,
                                                       table=table)}
        return

    def db_bulk_insert(self,
                       sql=None,
                       database=None,
                       table=None,
                       values=None,
                       force_commit=False):
        """
        Insert a set of data when there are enough data or when the
        force_commit is True
        Retrurn the remaining values to insert
        """
        values = values or []
        # execute and commit if we have enough data to commit(bulk_insert) or
        # if commit is forced
        if len(values) >= self.bulk_insert or (values and force_commit):
            logging.info("Processing bulk insert")
            count = self.db_request(sql=sql,
                                    values=values,
                                    database=database,
                                    table=table,
                                    foreign_key_check=False,
                                    execute_method='executemany')
            values = []
            logging.info("%s rows inserted into %s.%s", count, database, table)

        return values

    def write(self, database=None, table=None, data=None):
        """
        Write method implemented which is in charge of writing data from
        Source into archive database. It calls the db_bulk_insert method to
        write by set of data
        """
        if not self.archive_data:
            logging.info(
                "Ignoring data archiving because archive_data is "
                "set to % s", self.archive_data)
            return

        self.prerequisites(database=database, table=table)
        primary_key = self.get_table_primary_key(database=database,
                                                 table=table)

        values = []
        for item in data:
            placeholders = ', '.join(['%s'] * len(item))
            columns = '`' + '`, `'.join(item.keys()) + '`'
            sql = "INSERT INTO {database}.{table} ({columns}) VALUES "\
                "({placeholders}) ON DUPLICATE KEY UPDATE {pk} = {pk}".format(
                    database=self.archive_db_name,
                    table=table,
                    columns=columns,
                    placeholders=placeholders,
                    pk=primary_key)
            values.append([v for v in item.values()])
            values = self.db_bulk_insert(sql=sql,
                                         values=values,
                                         database=self.archive_db_name,
                                         table=table)

        # Force commit of remaining data even if we do not reach the
        # bulk_insert limit
        self.db_bulk_insert(sql=sql,
                            database=self.archive_db_name,
                            table=table,
                            values=values,
                            force_commit=True)
        return

    def clean_exit(self):
        """
        Tasks to be executed to exit cleanly
        - disconnect from the db
        """
        logging.info("Closing destination DB connection")
        self.disconnect()
