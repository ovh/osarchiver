# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
Configuration class to handle osarchiver config
"""

import re
import logging
import configparser

from osarchiver.archiver import Archiver
from osarchiver.destination import factory as dst_factory
from osarchiver.source import factory as src_factory

BOOLEAN_OPTIONS = ['delete_data', 'archive_data', 'enable', 'foreign_key_check']


class Config():
    """
    This class is able to read an ini configuration file and instanciate
    Archivers to be run
    """

    def __init__(self, file_path=None, dry_run=False):
        self.file_path = file_path
        """
        Config class instantiator. Instantiate a configparser
        """
        self.parser = configparser.ConfigParser(
            interpolation=configparser.ExtendedInterpolation())
        self.loaded = 0
        self._archivers = []
        self._sources = []
        self._destinations = []
        self.dry_run = dry_run

    def load(self, file_path=None):
        """
        Load a file given in arguments and make the configparser read it
        and return the parser
        """

        if self.loaded == 1:
            return self.parser

        if file_path is not None:
            self.file_path = file_path

        logging.info("Loading configuration file %s", self.file_path)
        loaded_files = self.parser.read(self.file_path)
        self.loaded = len(loaded_files)
        logging.debug("Config object loaded")
        logging.debug(loaded_files)
        return self.parser

    def sections(self):
        """
        return call to sections() of ConfigParser for the config file
        """
        if self.loaded == 0:
            self.load()
        return self.parser.sections()

    def section(self, name, default=True):
        """
        return a dict of key/value for the given section
        if defaults is set to False, it will remove defaults value from the section
        """
        if not name or not self.parser.has_section(name):
            return {}
        default_keys = []
        if not default:
            default_keys = [k for k, v in self.parser.items('DEFAULT')]
        return {
            k: v for k, v in self.parser.items(name) if k not in default_keys
        }

    @property
    def archivers(self):
        """
        This method load the configuration and instantiate all the Source and
        Destination objects needed for each archiver
        """
        self.load()
        if self._archivers:
            return self._archivers

        archiver_sections = [
            a for a in self.sections() if str(a).startswith('archiver:')
        ]

        def args_factory(section):
            """
            Generic function that takes a section from configuration file
            and return arguments that are passed to source or destination
            factory
            """
            args_factory = {
                k: v if k not in BOOLEAN_OPTIONS else self.parser.getboolean(
                    section, k)
                for (k, v) in self.parser.items(section)
            }
            args_factory['name'] = re.sub('^(src|dst):', '', section)
            args_factory['dry_run'] = self.dry_run
            args_factory['conf'] = self
            logging.debug(
                "'%s' factory parameters: %s", args_factory['name'], {
                    k: v if k != 'password' else '***********'
                    for (k, v) in args_factory.items()
                })

            return args_factory

        # Instanciate archivers:
        # One archiver is bascally a process of archiving
        # One archiver got one source and at least one destination
        # It means we have a total of source*count(destination)
        # processes to run per archiver
        for archiver in archiver_sections:
            # If enable: 0 in archiver config ignore it
            if not self.parser.getboolean(archiver, 'enable'):
                logging.info("Archiver %s is disabled, ignoring it", archiver)
                continue

            # src and dst sections are comma, semicolon, or carriage return
            # separated name
            src_sections = [
                'src:{}'.format(i.strip())
                for i in re.split(r'\n|,|;', self.parser[archiver]['src'])
            ]

            # destination is not mandatory
            # usefull to just delete data from DB
            dst_sections = [
                'dst:{}'.format(i.strip()) for i in re.split(
                    r'\n|,|;', self.parser[archiver].get('dst', '')) if i
            ]

            for src_section in src_sections:
                src_args_factory = args_factory(src_section)
                src = src_factory(**src_args_factory)
                destinations = []
                for dst_section in dst_sections:
                    dst_args_factory = args_factory(dst_section)
                    dst_args_factory['source'] = src
                    dst = dst_factory(**dst_args_factory)
                    destinations.append(dst)

                self._archivers.append(
                    Archiver(name=re.sub('^archiver:', '', archiver),
                             src=src,
                             dst=destinations,
                             conf=self))

        return self._archivers

    @property
    def sources(self):
        """
        Return a list of Sources object after having loaded the
        configuration file
        """
        self.load()
        self._sources.extend(
            [s for s in self.sections() if str(s).startswith('src:')])
        return self._sources

    @property
    def destinations(self):
        """
        Return a list of Destinations object after having loaded the
        configuration file
        """
        self.load()
        self._destinations.extend(
            [d for d in self.sections() if str(d).startswith('dst:')])
        return self._destinations
