# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
Base class file of file backend implementation.
"""

import logging
import os
import shutil
import re
from importlib import import_module
from abc import ABCMeta, abstractmethod
import arrow
from osarchiver.destination.base import Destination


class File(Destination):
    """
    The base File class is a Destination like class which implement file
    backend.
    """

    def __init__(self,
                 directory=None,
                 archive_format='tar',
                 formats=None,
                 dry_run=False,
                 source=None,
                 **kwargs):
        """
        Initiator
        :param str directory: the directory where store the files
        :param str archive_format: which format to use to compress file
        default is tar, format available are formats available with
        shutil.make_archive
        :param list formats: list of formats in which data will be written.
        The format should be implemented as a subclass of the current class, it
        is called a formatter
        :param bool dry_run: if enable  will not write for real
        :param source: the Source instance
        """
        formats = formats or ['csv']
        # Archive formats: zip, tar, gztar, bztar, xztar
        Destination.__init__(self, backend='file')
        self.directory = str(directory).format(
            date=arrow.now().strftime('%F_%T'))
        self.archive_format = archive_format
        self.formats = re.split(r'\n|,|;', formats)
        self.formatters = {}
        self.source = source
        self.dry_run = dry_run

        self.init()

    def close(self):
        """
        This method close will call close() method of each formatter
        """
        for formatter in self.formatters:
            getattr(self.formatters[formatter], 'close')()

    def clean_exit(self):
        """
        clean_exit method that should be implemented. Close all formatter and
        compress file
        """
        self.close()
        self.compress()
        if self.dry_run:
            try:
                logging.info(
                    "Removing target directory %s because dry-run "
                    "mode enabled", self.directory)
                os.rmdir(self.directory)
            except OSError as oserror_exception:
                logging.error(
                    "Unable to remove dest directory (certainly not "
                    "empty dir): %s", oserror_exception)

    def files(self):
        """
        Return a list of files open by all formatters
        """
        files = []
        for formatter in self.formatters:
            files.extend(getattr(self.formatters[formatter], 'files')())

        return files

    def compress(self):
        """
        Compress all the files open by formatters
        """
        for file_to_compress in self.files():
            logging.info("Archiving %s using %s format", file_to_compress,
                         self.archive_format)
            compressed_file = shutil.make_archive(
                file_to_compress,
                self.archive_format,
                root_dir=os.path.dirname(file_to_compress),
                base_dir=os.path.basename(file_to_compress),
                dry_run=self.dry_run)

            if compressed_file:
                logging.info("Compressed file available at %s",
                             compressed_file)
                os.remove(file_to_compress)

    def init(self):
        """
        init stuff
        """
        os.makedirs(self.directory)

    def write(self, database=None, table=None, data=None):
        """
        Write method that should be implemented
        For each format instanciate a formatter and writes the data set
        """
        logging.info("Writing on backend %s %s data length", self.backend,
                     len(data))

        for write_format in self.formats:
            # initiate formatter
            if write_format not in self.formatters:
                try:
                    class_name = write_format.capitalize()
                    module = import_module(
                        'osarchiver.destination.file.{write_format}'.format(
                            write_format=write_format))
                    formatter_class = getattr(module, class_name)
                    formatter_instance = formatter_class(
                        directory=self.directory,
                        dry_run=self.dry_run,
                        source=self.source)
                    self.formatters[write_format] = formatter_instance
                except (AttributeError, ImportError) as my_exception:
                    logging.error(my_exception)
                    raise ImportError(
                        "{} is not part of our file formatter".format(
                            write_format))
                else:
                    if not issubclass(formatter_class, Formatter):
                        raise ImportError(
                            "Unsupported '{}' file format ".format(
                                write_format))

            writer = self.formatters[write_format]
            writer.write(database=database, table=table, data=data)


class Formatter(metaclass=ABCMeta):
    """
    Formatter base class which implements a backend, each backend have to
    inherit from that class
    """

    def __init__(self, name=None, directory=None, dry_run=None, source=None):
        """
        Initiator:

        """
        self.directory = directory
        self.source = source
        self.handlers = {}
        self.now = arrow.now().strftime('%F_%T')
        self.dry_run = dry_run
        self.name = name or type(self).__name__.upper()

    def files(self):
        """
        Return the list of file handlers
        """
        return [self.handlers[h]['file'] for h in self.handlers]

    @abstractmethod
    def write(self, data=None):
        """
        Write method that should be implemented by the classes that inherit
        from the import formatter class
        """

    def close(self):
        """
        The method close all the file handler which are not closed
        """
        for handler in self.handlers:
            if self.handlers[handler]['fh'].closed:
                continue
            logging.info("Closing handler of %s",
                         self.handlers[handler]['file'])
            self.handlers[handler]['fh'].close()
            self.handlers[handler]['fh'].close()
