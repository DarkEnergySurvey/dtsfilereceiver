#!/usr/bin/env python
"""
Data transfer from NOAO
"""
import argparse
from dtsfilereceiver.decadeFileTransfer import DecadeFileTransfer
import datetime

def parse_options():
    """ Parse any command line options

        Returns
        -------
        Tuple containing the options and argumnts
    """
    parser = argparse.ArgumentParser(description='Monitor the backup status and software')
    parser.add_argument('--config', action='store', required=True)
    args = vars(parser.parse_args())  # convert to dict
    return args

def main():
    """ Main entry point
    """
    args = parse_options()
    filehandler = DecadeFileTransfer(args['config'])

    filehandler.initiate_connection()

    filehandler.update_transfers()

    filehandler.get_new_nights()


    if not filehandler.things_to_do():
        now = datetime.datetime.now()
        if now.hour == 10:
            filehandler.get_dirs()
            filehandler.load_new_dirs()
            if not filehandler.things_to_do():
                filehandler.report()
                return

    filehandler.get_dirs()
    filehandler.load_new_dirs()

    filehandler.process_nights()

    filehandler.process_projects()
    filehandler.report()


if __name__ == '__main__':
    main()
