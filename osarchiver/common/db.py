# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
DB base class file which provide helpers and common method for Source and
Destination Db backend

The class provide a metadata storage to prevent doing some compute
several times. It also keeps a reference on pymysq.cursor per table to
avoid creating too much cursor
"""

import logging
import re
import warnings
import time
import timeit
# need to include datetime to handle some result
# of pymysql (integrity exception helpers)
import datetime
import pymysql


class DbBase():
    """
    The DbBase class that should be inherited from Source and Destination Db
    backend
    """

    def __init__(self,
                 host=None,
                 user=None,
                 password=None,
                 select_limit=1000,
                 delete_limit=500,
                 port=3306,
                 dry_run=False,
                 deleted_column=None,
                 max_retries=5,
                 bulk_insert=1000,
                 retry_time_limit=2,
                 delete_loop_delay=2,
                 **kwargs):
        """
        instantiator of database base class
        """
        self.host = host
        self.user = user
        self.port = int(port)
        self.password = password
        self.delete_limit = int(delete_limit)
        self.deleted_column = deleted_column
        self.connection = None
        self.select_limit = int(select_limit)
        self.bulk_insert = int(bulk_insert)
        self.dry_run = dry_run
        self.metadata = {}
        # number of retries when an error occure
        self.max_retries = max_retries
        # how long wait between two retry
        self.retry_time_limit = retry_time_limit
        self.delete_loop_delay = delete_loop_delay

        # hide some warnings we do not care
        warnings.simplefilter("ignore")
        self.connect()

    def connect(self):
        """
        connect to the database and set the connection attribute to
        pymysql.connect
        """
        self.connection = pymysql.connect(host=self.host,
                                          user=self.user,
                                          port=self.port,
                                          password=self.password,
                                          database=None)
        logging.debug("Successfully connected to mysql://%s:%s@%s:%s",
                      self.user, '*' * len(self.password), self.host,
                      self.port)

    def disconnect(self):
        """
        disconnect from the databse if connection is open
        """
        if self.connection.open:
            self.connection.close()

    def add_metadata(self, database=None, table=None, key=None, value=None):
        """
        store for one database/table a key with a value
        """
        if database not in self.metadata:
            self.metadata[database] = {}

        if table not in self.metadata[database]:
            self.metadata[database][table] = {}

        logging.debug("Adding metadata %s.%s.%s = %s", database, table, key,
                      value)

        self.metadata[database][table][key] = value
        return self.metadata[database][table][key]

    def get_metadata(self, database=None, table=None, key=None):
        """
        return the key's value for a database.table
        """
        if database is None or table is None:
            return None

        if database in self.metadata and table in self.metadata[database]:
            return self.metadata[database][table].get(key)

        return None

    def disable_fk_check(self, cursor=None):
        """
        Disable foreign key check for a cursor
        """
        logging.debug("Disabling foreign_key_check")
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

    def enable_fk_check(self, cursor=None):
        """
        Enable foreign key check for a cursor
        """
        logging.debug("Enabling foreign_key_check")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    def check_request_retry(self):
        """
        When an SQL error occured, this method is called and do some check
        Right now it only re-open connection if the connection is closed
        """
        logging.debug("Sleeping %s sec before retrying....",
                      self.retry_time_limit)
        time.sleep(self.retry_time_limit)
        # Handle auto reconnect
        if not self.connection.open:
            logging.info("Re-opening connection which seems abnormaly "
                         "closed")
            self.connect()

    def set_foreign_key_check(self,
                              foreign_key_check=None,
                              cursor=None,
                              database=None,
                              table=None,
                              new_cursor=False):
        """
        This method set the correct value to foreign key check. Instead of
        executing it at each requests which is time consuming and overloading
        it checks in metadata the current value and change it if needed
        """
        fk_check_in_cache = False
        current_fk_check = self.get_metadata(
            database=database,
            table=table,
            key='fk_check_{c}'.format(c=cursor))

        # nothing in cache we want to apply the foreign_key_check value
        # set current_fk_check to negate of foreign_key_check
        if current_fk_check is None or new_cursor is True:
            logging.debug("foreign key check value not found in cache")
            current_fk_check = not foreign_key_check

        if foreign_key_check is False and current_fk_check is True:
            self.disable_fk_check(cursor=cursor)
        elif foreign_key_check is True and current_fk_check is False:
            self.enable_fk_check(cursor=cursor)
        else:
            fk_check_in_cache = True

        if database is not None \
                and table is not None and not fk_check_in_cache:
            self.add_metadata(database=database,
                              table=table,
                              key='fk_check_{c}'.format(c=cursor),
                              value=foreign_key_check)

    def get_cursor(self,
                   database=None,
                   table=None,
                   cursor_type=None,
                   new=False,
                   fk_check=None):
        """
        Return the pymysql cursor mapped to a database.table if exists in
        metadata otherwise it create a new cursor
        """
        default_cursor_type = pymysql.cursors.Cursor
        cursor_type = cursor_type or default_cursor_type
        cursor = None
        cursor_in_cache = False
        # open db connection if not opened
        if not self.connection.open:
            self.connect()
            cursor = self.connection.cursor(cursor_type)
        else:
            # if this is not a cursor creation
            # try to get the cached one from metadata
            if not new:
                cursor = self.get_metadata(
                    database=database,
                    table=table,
                    key='cursor_{c}'.format(c=cursor_type))

            # if cursor is None (creation or not found in metadata)
            # set the cursor type to default one
            type_of_cursor = type(cursor)
            if cursor is None:
                type_of_cursor = default_cursor_type

            # Check if the cursor retrieved is well typed
            # if not force the cursor to be unset
            # it will be re-created after
            if cursor is not None and cursor_type != type_of_cursor:
                logging.debug(
                    "Type of cursor found in cache is %s, we want %s"
                    "  instead, need to create a new cursor", type_of_cursor,
                    cursor_type)
                cursor = None

            # cursor creation
            if cursor is None:
                logging.debug("No existing cursor found, creating a new one")
                cursor = self.connection.cursor(cursor_type)
            else:
                cursor_in_cache = True
                logging.debug("Using cached cursor %s", cursor)
        # set the foreign key check value if needed
        # for the cursor
        if fk_check is not None:
            self.set_foreign_key_check(cursor=cursor,
                                       database=database,
                                       table=table,
                                       foreign_key_check=fk_check,
                                       new_cursor=new)
        # Add the cursor in cache
        if database is not None and table is not None and not cursor_in_cache:
            logging.debug("Caching cursor for %s.%s", database, table)
            self.add_metadata(database=database,
                              table=table,
                              key='cursor_{c}'.format(c=cursor_type),
                              value=cursor)
        return cursor

    def _db_execute(self, sql=None, cursor=None, method=None, values=None):
        """
        Execute a request on database
        """
        logging.debug("Executing SQL command: '%s'", sql)
        # execute / execute_many method
        start = timeit.default_timer()
        getattr(cursor, method)(sql, values)
        end = timeit.default_timer()
        logging.debug("SQL duration: %s sec", end - start)

    def _db_fetch(self, fetch_method=None, cursor=None, fetch_args=None):
        """
        This method fetch data in database
        """
        start = timeit.default_timer()
        fetched_values = getattr(cursor, fetch_method)(**fetch_args)
        end = timeit.default_timer()
        logging.debug("Data fetch duration: %s sec", end - start)
        return fetched_values

    def _db_commit(self, cursor=None, sql=None, values_length=None):
        """
        Commit the executed request, return the number of row modified
        """
        if self.dry_run:
            logging.info(
                "[DRY RUN]: here is what I should have "
                "commited: '%s'", cursor.mogrify(query=sql))
            self.connection.rollback()
            return values_length
        # Not dry-run mode: commit the request
        # return the number of row affected by the request
        start = timeit.default_timer()
        self.connection.commit()
        end = timeit.default_timer()
        logging.debug("Commit duration: %s sec", end - start)
        return cursor.rowcount

    def db_request(self,
                   sql=None,
                   values=None,
                   fetch_method=None,
                   fetch_args=None,
                   database=None,
                   table=None,
                   cursor_type=None,
                   foreign_key_check=None,
                   execute_method='execute'):
        """
        generic method to do a request to the db
        It handles a retry on failure, execept for foreign key exception which
        in our case useless
        In case of error connection, it sleeps 20 seconds before retrying
        """
        retry = 0
        cursor = None
        force_cursor_creation = False
        values = values or []

        fetch_args = fetch_args or {}
        if self.dry_run:
            foreign_key_check = False
            logging.debug("Force disabling foreign key check because we are in"
                          " dry run mode")

        while retry <= self.max_retries:
            try:
                if retry > 0:
                    logging.info("Retry %s/%s", retry, self.max_retries)
                    self.check_request_retry()

                if cursor is None:
                    cursor = self.get_cursor(database=database,
                                             table=table,
                                             cursor_type=cursor_type,
                                             fk_check=foreign_key_check,
                                             new=force_cursor_creation)

                if database is not None:
                    self.connection.select_db(database)

                # Execute the query
                self._db_execute(sql=sql,
                                 cursor=cursor,
                                 method=execute_method,
                                 values=values)

                # Fetch and return the data
                if fetch_method is not None:
                    return self._db_fetch(fetch_method=fetch_method,
                                          cursor=cursor,
                                          fetch_args=fetch_args)
                # no fetch_method means we need to commit the request
                # In dry_run mode just display what would have been commited
                return self._db_commit(cursor=cursor,
                                       sql=sql,
                                       values_length=len(values))

            except pymysql.Error as sql_exception:
                logging.error("SQL error: %s", sql_exception.args)
                if sql_exception.args[0] == "(0, '')":
                    logging.debug("Cursor need to be recreated")
                    if cursor is not None:
                        cursor.close()
                    cursor = None
                    force_cursor_creation = True
                # foreign key constraint error, there is no sense in continuing
                if sql_exception.args[0] == 1451:
                    logging.debug("Foreign key constraint error no retry "
                                  "attempted")
                    retry = self.max_retries
                if sql_exception.args[0] == 2003:
                    self.connection.close()
                    logging.error("MySQL connection error, sleeping 20 "
                                  "seconds before reconnecting...")
                retry += 1
                if retry > self.max_retries:
                    raise sql_exception
                continue

    def get_os_databases(self):
        """
        Return a list of databases available
        """
        sql = "SHOW DATABASES"
        result = self.db_request(sql=sql, fetch_method='fetchall')
        logging.debug("DB result: %s", result)
        return [i[0] for i in result]

    def get_database_tables(self, database=None):
        """
        Return a list of tables available for a database
        """
        if database is None:
            logging.warning(
                "Can not call get_database_tables on None database")
            return []

        sql = "SHOW TABLES"
        return self.db_request(sql=sql,
                               database=database,
                               fetch_method='fetchall')

    def table_has_column(self, database=None, table=None, column=None):
        """
        Return True/False after checking that a column exists in a table
        """
        sql = "SELECT column_name FROM information_schema.columns WHERE "\
            "table_schema='{db}' and table_name='{table}' AND "\
            "column_name='{column}'".format(
                db=database, table=table, column=column)
        return bool(
            self.db_request(sql=sql,
                            fetch_method='fetchall',
                            database=database,
                            table=table))

    def table_has_deleted_column(self, database=None, table=None):
        """
        Return True/False depending if the table has the deleted column
        """
        return self.table_has_column(database=database,
                                     table=table,
                                     column=self.deleted_column)

    def get_table_primary_key(self, database=None, table=None):
        """
        Return the first primary key of a table
        Store the pk in metadata and return it if exists
        """
        primary_key = self.get_metadata(database=database,
                                        table=table,
                                        key='primary_key')
        if primary_key is not None:
            return primary_key

        sql = "SHOW KEYS FROM {db}.{table} WHERE "\
            "Key_name='PRIMARY'".format(db=database, table=table)
        # Dirty but .... Column name is the 5 row
        primary_key = self.db_request(sql=sql, fetch_method='fetchone')[4]
        logging.debug("Primary key of %s.%s is %s", database, table,
                      primary_key)
        self.add_metadata(database=database,
                          table=table,
                          key='primary_key',
                          value=primary_key)
        return primary_key

    def get_tables_with_fk(self, database=None, table=None):
        """
        For a given table return a list of foreign key
        """
        sql = "SELECT table_schema, table_name, column_name "\
            "FROM information_schema.key_column_usage "\
            "WHERE referenced_table_name IS NOT NULL" \
            " AND referenced_table_schema='{db}'"\
            " AND referenced_table_name='{table}'".format(
                db=database, table=table)

        result = self.db_request(sql=sql,
                                 fetch_method='fetchall',
                                 cursor_type=pymysql.cursors.DictCursor)
        if result:
            logging.debug("Table %s.%s have child tables with foreign key: %s",
                          database, table, result)
        else:
            logging.debug(
                "Table %s.%s don't have child tables with foreign "
                "key", database, table)
        return result

    def sql_integrity_exception_parser(self, error):
        """
        Parse a foreign key integrity exception and return a dict of pattern
        with useful information
        """
        result = {}
        regexp = r'^.+fails \(`'\
            r'(?P<db>.+)`\.`'\
            r'(?P<table>.+)`, CONSTRAINT `.+`'\
            r' FOREIGN KEY \(`'\
            r'(?P<fk>.+)`\) REFERENCES `'\
            r'(?P<ref_table>.+)` \(`'\
            r'(?P<ref_column>.+)`\)\)$'
        match = re.match(regexp, error)
        if match:
            result = match.groupdict()
        else:
            logging.warning("SQL error '%s' does not match regexp "
                            "'%s'", error, regexp)
        return result

    def integrity_exception_select_statement(self, error="", row=None):
        """
        Parse a foreign key excpetion  and return a SELECT statement to
        retrieve the offending children rows
        """
        row = row or {}
        data = self.sql_integrity_exception_parser(error)
        # empty dict is when failing to parse exception
        if not data:
            return "Unable to parse exception, here data: "\
                "{row}".format(row=row)

        return "SELECT * FROM `{db}`.`{table}` WHERE `{fk}` = "\
            "'{value}'".format(value=row[data['ref_column']],
                               **data)

    def integrity_exception_potential_fix(self, error="", row=None):
        """
        Parse a foerign key exception and return an UPDATE sql statement that
        mark non deleted children data as deleted
        """
        row = row or {}
        data = self.sql_integrity_exception_parser(error)
        if not data:
            return "Unable to parse exception, here data: "\
                "{row}".format(row=row)

        update = "UPDATE `{db}`.`{table}` INNER JOIN `{db}`.`{ref_table}` ON "\
            "`{db}`.`{ref_table}`.`{ref_column}` = `{db}`.`{table}`.`{fk}` "\
            "SET `{db}`.`{table}`.`{deleted_column}` = "\
            "`{db}`.`{ref_table}`.`{deleted_column}` WHERE {fk} = "

        if str(row[data['ref_column']]).isdigit():
            update += "{value}"
        else:
            update += "'{value}'"

        update += " AND `{db}`.`{table}`.`{deleted_column}` IS NULL"
        update = update.format(deleted_column=self.deleted_column,
                               value=row[data['ref_column']],
                               **data)

        return update
