#!/usr/bin/env python
import argparse
from dtsfilereceiver.decadeDuplicateProcess import DecadeDuplicateProcess


def parse_options():
    """ Parse any command line options

        Returns
        -------
        Tuple containing the options and argumnts
    """
    parser = argparse.ArgumentParser(description='Monitor the backup status and software')
    parser.add_argument('--config', action='store', required=True)
    parser.add_argument('--logfile', action='store', required=True)
    args = vars(parser.parse_args())  # convert to dict
    return args



def main():
    args = parse_options()

    dup = DecadeDuplicateProcess(args['config'], args['logfile'])
    dup.process_log()
    dup.report()

if __name__ == '__main__':
    main()
