# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.
# Copyright 2019 The OSArchiver Authors. All rights reserved.
"""
main file providing osarchiver program
"""

import sys
import os
import logging
import argparse
import traceback

from osarchiver.config import Config


def parse_args():
    """
    function to parse CLI arguments
    return parse_args() of ArgumentParser
    """
    parser = argparse.ArgumentParser()

    def file_exists(one_file):
        if not os.path.exists(one_file):
            raise argparse.ArgumentTypeError(
                '{f} no such file'.format(f=one_file))
        return one_file

    parser.add_argument('--config',
                        help='Configuration file to read',
                        default=None,
                        required=True,
                        type=file_exists)
    parser.add_argument('--log-file',
                        help='Append log to the specified file',
                        default=None)
    parser.add_argument('--log-level',
                        help='Set log level',
                        choices=['info', 'warn', 'error', 'debug'],
                        default='info')
    parser.add_argument('--debug',
                        help='Enable debug mode',
                        default=False,
                        action='store_true')
    parser.add_argument('--dry-run',
                        help='Display what would be done without'
                        ' really deleting or writing data',
                        default=False,
                        action='store_true')
    args = parser.parse_args()

    if args.debug:
        args.log_level = 'debug'

    return args


def configure_logger(level='info', log_file=None):
    """
    function that configure logging module
    """
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper()))

    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s: %(message)s')

    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    if log_file is not None:
        file_handler = logging.FileHandler(filename=log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


def run():
    """
    main function that is called when running osarchiver script
    It parses arguments, configure logging, load the configuration file and for
    each archiver call the run() method
    """
    try:
        args = parse_args()
        config = Config(file_path=args.config, dry_run=args.dry_run)
        configure_logger(level=args.log_level, log_file=args.log_file)

        for archiver in config.archivers:
            logging.info("Running archiver %s", archiver.name)
            archiver.run()
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt detected")
        for archiver in config.archivers:
            archiver.clean_exit()
        return 1
    except Exception as my_exception:
        logging.error(my_exception)
        logging.error("Full traceback is: %s", traceback.format_exc())
        for archiver in config.archivers:
            archiver.clean_exit()
        return 1
    return 0
