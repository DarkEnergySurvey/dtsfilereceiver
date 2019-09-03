#!/usr/bin/env python
"""
Data transfer from NOAO
"""
import argparse
from dtsfilereceiver.decadeFastTransfer import DecadeFastTransfer
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
    filehandler = DecadeFastTransfer(args['config'])

    filehandler.initiate_connection()

    filehandler.update_transfers()

    filehandler.get_new_files()

    filehandler.get_dirs()

    filehandler.load_new_files()

    if not filehandler.things_to_do():
        filehandler.report()
        return

    filehandler.process_files()
    filehandler.report()


if __name__ == '__main__':
    main()
