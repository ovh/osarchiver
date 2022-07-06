# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
OSArchiver's Source class that implement a db backend
"""

import re
import time
import logging
import pymysql
import arrow
from numpy import array_split
from osarchiver.source import Source
from osarchiver.common.db import DbBase
from sqlalchemy import inspect
import sqlalchemy_utils

NOT_OS_DB = ['mysql', 'performance_schema', 'information_schema']


class Db(Source, DbBase):
    """
    Database backend of OSArchiver's Source
    """

    def __init__(self,
                 databases=None,
                 tables=None,
                 delete_data=0,
                 excluded_databases='',
                 excluded_tables='',
                 where='1=1 LIMIT 0',
                 archive_data=None,
                 name=None,
                 destination=None,
                 **kwargs):
        """
        Create a Source instance with relevant configuration parameters given
        in arguments
        """
        self.databases = databases
        self.tables = tables
        self.configured_excluded_databases = [
            d for d in re.split(',|;|\n', excluded_databases.replace(' ', ''))
        ]
        self._excluded_databases = None
        self.configured_excluded_tables = [
            d for d in re.split(',|;|\n', excluded_tables.replace(' ', ''))
        ]
        self._excluded_tables = None
        self.archive_data = archive_data
        self.delete_data = delete_data
        self.destination = destination
        self._databases_to_archive = []
        self._tables_to_archive = {}
        self.tables_with_circular_fk = []
        # When selecting data be sure to use the same date to prevent selecting
        # parent data newer than children data, it is of the responsability of
        # the operator to use the {now} formating value in the configuration
        # file in the where option. If {now} is ommitted it it is possible to
        # get foreign key check errors because of parents data newer than
        # children data
        self.now = arrow.utcnow().format(fmt='YYYY-MM-DD HH:mm:ss')
        self.where = where.format(now=self.now)
        Source.__init__(self, backend='db', name=name,
                        conf=kwargs.get('conf', None))
        DbBase.__init__(self, **kwargs)

    def __repr__(self):
        return "Source {name} [Backend:{backend} Host:{host} - DB:{db} - "\
            "Tables:{tables}]".format(backend=self.backend, db=self.databases,
                                      name=self.name, tables=self.tables,
                                      host=self.host)

    @property
    def excluded_databases(self):
        if self._excluded_databases is not None:
            return self._excluded_databases

        excluded_db_set = set(self.configured_excluded_databases)
        excluded_db_set.update(set(NOT_OS_DB))
        self._excluded_databases = list(excluded_db_set)

        return self._excluded_databases

    @property
    def excluded_tables(self):
        if self._excluded_tables is not None:
            return self._excluded_tables

        self._excluded_tables = self.configured_excluded_tables

        return self._excluded_tables

    def databases_to_archive(self):
        """
        Return a list of databases that are eligibles to archiving. If no
        database are provided or the * character is used the method basically
        do a SHOW DATABASE to get available databases
        The method exclude the databases that are explicitly excluded
        """
        if self._databases_to_archive:
            return self._databases_to_archive

        if self.databases is None or self.databases == '*':
            self._databases_to_archive = self.get_os_databases()
        else:
            self._databases_to_archive = [
                d for d in re.split(',|;|\n', self.databases.replace(' ', ''))
            ]

        excluded_databases_regex = \
            "^(" + "|".join(self.excluded_databases) + ")$"
        self._databases_to_archive = [
            d for d in self._databases_to_archive
            if not re.match(excluded_databases_regex, d)
        ]

        return self._databases_to_archive

    def tables_to_archive(self, database=None):
        """
        For a given database, return the list of tables that are eligible to
        archiving.
        - Retrieve tables if needed (*, or empty)
        - Check that tables has 'deleted_at' column (deleted_column
        parameter)
        - Exclude tables in excluded_tables
        - Reorder tables depending foreign key
        """
        if database is None:
            logging.warning("Can not call tables_to_archive on None database")
            return []
        if database in self._tables_to_archive:
            return self._tables_to_archive[database]

        database_tables = [
            v[0] for (i, v) in enumerate(self.get_database_tables(database))
        ]
        logging.info("Tables list of database '%s': %s", database,
                     database_tables)
        # Step 1: is to get all the tables we want to archive
        # no table specified or jocker used means we want all tables
        # else we filter against the tables specified
        if self.tables is None or self.tables == '*':
            self._tables_to_archive[database] = database_tables
        else:
            self._tables_to_archive[database] = \
                [t for t in re.split(',|;|\n', self.tables.replace(' ', ''))
                 if t in database_tables]

        # Step 2: verify that all tables have the deleted column 'deleted_at'
        logging.debug("Verifying that tables have the '%s' column",
                      self.deleted_column)
        tables = []
        for table in self._tables_to_archive[database]:
            if not self.table_has_deleted_column(table=table,
                                                 database=database):
                logging.debug(
                    "Table '%s' has no column named '%s',"
                    " ignoring it", table, self.deleted_column)
                continue
            tables.append(table)
        # update self._tables_to_archive with the filtered tables
        self._tables_to_archive[database] = tables

        # Step 3: then exclude the one explicitly given
        excluded_tables_regex = "^(" + "|".join(self.excluded_tables) + ")$"
        logging.debug("Ignoring tables matching '%s'", excluded_tables_regex)
        self._tables_to_archive[database] = [
            t for t in self._tables_to_archive[database]
            if not re.match(excluded_tables_regex, t)
        ]

        # Step 4 for each table retrieve child tables referencing the parent
        # table and order them childs first, parents then
        sorted_tables = self.sort_tables(
            database=database, tables=self._tables_to_archive[database])
        self._tables_to_archive[database] = sorted_tables

        logging.debug(
            "Tables ordered depending foreign key dependencies: "
            "'%s'", self._tables_to_archive[database])
        return self._tables_to_archive[database]

    def sort_tables(self, database=None, tables=[]):
        """
        Given a DB and a list of tables return the list orderered depending
        foreign key check in order to get child table before parent table
        """
        inspector = inspect(self.sqlalchemy_engine)
        sorted_tables = []
        logging.debug("Tables to sort: %s", sorted_tables)
        for table in tables:
            if not self.table_has_deleted_column(table=table, database=database):
                continue
            if table not in sorted_tables:
                logging.debug("Table %s added to final list", table)
                sorted_tables.append(table)
            idx = sorted_tables.index(table)
            fks = inspector.get_foreign_keys(table, schema=database)
            logging.debug("Foreign keys of %s: %s", table, fks)
            for fk in fks:
                t = fk['referred_table']

                if t in sorted_tables:
                    if sorted_tables.index(t) > idx:
                        continue
                    else:
                        sorted_tables.remove(t)
                sorted_tables.insert(idx+1, t)

        return sorted_tables

    def select(self, limit=None, database=None, table=None):
        """
        select data from a database.table, apply limit or take the default one
        the select by set depends of the primary key type (int vs uuid)
        In case of int:
            SELECT * FROM <db>.<table> WHERE <pk> > <last_selected_id> AND ...
        In case of uuid (uuid are not ordered naturally ordered, we sort them)
            SELECT * FROM <db>.<table> WHERE <pk> > "<last_selected_id>" AND...
            ORDER BY <pk>
        """
        offset = 0
        last_selected_id = 0

        # Use primary key column to improve performance on large
        # dataset vs using OFFSET
        primary_key = self.get_table_primary_key(database=database,
                                                 table=table)

        if limit is None:
            limit = self.select_limit

        sql = "SELECT * FROM `{database}`.`{table}` WHERE {pk} > "\
            "'{last_id}' AND {where} LIMIT {limit}"

        pk_type_checked = False

        while True:
            formatted_sql = sql.format(database=database,
                                       table=table,
                                       where=self.where,
                                       limit=limit,
                                       last_id=last_selected_id,
                                       pk=primary_key,
                                       offset=offset)
            result = self.db_request(sql=formatted_sql,
                                     cursor_type=pymysql.cursors.DictCursor,
                                     database=database,
                                     table=table,
                                     fetch_method='fetchall')
            logging.info("Fetched %s result in %s.%s", len(result), database,
                         table)
            if not result:
                break
            last_selected_id = result[-1][primary_key]

            yield result

            offset += len(result)
            if pk_type_checked is False:
                # If the primary key is a digit remove the simple quote from
                # the last_id variable for performance purpose
                if str(last_selected_id).isdigit():
                    # remove the simple quote arround id
                    sql = "SELECT * FROM `{database}`.`{table}` WHERE {pk} >"\
                        " {last_id} AND {where} LIMIT {limit}"
                else:
                    # else this a string and we force to order by that string
                    # to simulate an integer primary key
                    sql = "SELECT * FROM `{database}`.`{table}` WHERE {pk} >"\
                        " '{last_id}' AND {where} ORDER BY {pk} LIMIT {limit}"

                pk_type_checked = True

    def read(self, limit=None):
        """
        The read method that has to be implemented (Source abstract class)
        """
        databases_to_archive = self.databases_to_archive()
        logging.info("Database elected for archiving: %s",
                     databases_to_archive)
        for database in databases_to_archive:
            tables_to_archive = self.tables_to_archive(database=database)
            logging.info("Tables elected for archiving: %s", tables_to_archive)
            for table in tables_to_archive:
                logging.info("%s.%s is to archive", database, table)
                yield {
                    'database':
                    database,
                    'table':
                    table,
                    'data':
                    self.select(limit=limit, database=database, table=table)
                }

    def delete_set(self, database=None, table=None, limit=None, data=None):
        """
        Delete a set of data using the primary_key of table
        """
        if not self.delete_data:
            logging.info(
                "Ignoring delete step because delete_data is set to"
                " %s", self.delete_data)
            return
        if limit is None:
            limit = self.delete_limit

        primary_key = self.get_table_primary_key(database=database,
                                                 table=table)

        # Check if primary key is a digit to prevent casting by MySQL and
        # optimize the request, store the value in metadata for caching
        pk_is_digit = self.get_metadata(database=database,
                                        table=table,
                                        key='pk_is_digit')
        if pk_is_digit is None:
            pk_is_digit = str(data[0][primary_key]).isdigit()
            self.add_metadata(database=database,
                              table=table,
                              key='pk_is_digit',
                              value=pk_is_digit)

        def create_array_chunks(array, chunk_size):
            for i in range(0, len(array), chunk_size):
                yield array[i:i + chunk_size]

        # For performance purpose split data in subdata of lenght=limit
        for subdata in list(create_array_chunks(data, limit)):
            if pk_is_digit:
                ids = ', '.join([str(d[primary_key]) for d in subdata])
            else:
                ids = '"' + '", "'.join([str(d['id']) for d in subdata]) + '"'

            total_deleted_count = 0
            # equivalent to a while True but we know why we are looping
            while "there are rows to delete":
                if total_deleted_count > 0:
                    logging.debug(
                        "Waiting %s seconds before deleting next"
                        "subset of data ", self.delete_loop_delay)
                    time.sleep(int(self.delete_loop_delay))

                sql = "DELETE FROM `{database}`.`{table}` WHERE "\
                    "`{pk}` IN ({ids}) LIMIT {limit}".format(
                        database=database,
                        table=table,
                        ids=ids,
                        pk=primary_key,
                        limit=limit)
                foreign_key_check = None
                if '{db}.{table}'.format(db=database, table=table) \
                    in self.tables_with_circular_fk:
                    foreign_key_check = False

                count = self.db_request(sql=sql,
                                        foreign_key_check=foreign_key_check,
                                        database=database,
                                        table=table)
                logging.info("%s rows deleted from %s.%s", count, database,
                             table)
                total_deleted_count += count

                if int(count) < int(limit) or \
                        total_deleted_count == len(subdata):
                    logging.debug("No more row to delete in this data set")
                    break

            logging.debug("Waiting %s seconds after a deletion",
                          self.delete_loop_delay)
            time.sleep(int(self.delete_loop_delay))

    def delete(self, database=None, table=None, limit=None, data=None):
        """
        The delete method that has to be implemented (Source abstract class)
        """
        try:
            self.delete_set(database=database,
                            table=table,
                            limit=limit,
                            data=data)
        except pymysql.err.IntegrityError as integrity_error:

            # foreign key constraint fails usually because of error while
            # processing openstack tasks
            # to prevent never deleting some of data, we re run delete with
            # half set of data if we caught an integrity error (1451)
            # To prevent never deleting rest of data of a set, we re run delete
            # with a half set if we caught an integrity error (1451)
            # until we caught the offending row
            if integrity_error.args[0] != 1451:
                raise integrity_error

            # we caught the row causing integrity error
            if len(data) == 1:
                logging.error("OSArchiver hit a row that will never be deleted"
                              " unless you fix remaining chlidren data")
                logging.error("Parent row that can not be deleted: %s", data)
                logging.error("To get children items:")
                logging.error(
                    self.integrity_exception_select_statement(
                        error=integrity_error.args[1], row=data[0]))
                logging.error("Here a POTENTIAL fix, ensure BEFORE that data "
                              "should be effectively deleted, then run "
                              "osarchiver again:")
                logging.error(
                    self.integrity_exception_potential_fix(
                        error=integrity_error.args[1], row=data[0]))
            else:
                logging.error("Integrity error caught, deleting with "
                              "dichotomy")
                for subdata in array_split(data, 2):
                    logging.debug(
                        "Dichotomy delete with a set of %s data "
                        "length", len(subdata))
                    # Add a sleep period because in case of error in delete_set
                    # we never sleep, it will avoid some lock wait timeout for
                    # incoming requests
                    time.sleep(int(self.delete_loop_delay))
                    self.delete(database=database,
                                table=table,
                                data=subdata,
                                limit=len(subdata))

    def clean_exit(self):
        """
        Tasks to be executed to exit cleanly:
        - Disconnect from the database
        """
        logging.info("Closing source DB connection")
        self.disconnect()
