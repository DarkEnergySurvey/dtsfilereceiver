#!/usr/bin/env python
import argparse
from dtsfilereceiver.decadeFilePrepare import DecadeFilePrepare


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
    args = parse_options()

    fprepare = DecadeFilePrepare(args['config'])
    fprepare.check_for_complete()
    fprepare.process_dirs()
    fprepare.report()

if __name__ == '__main__':
    main()
