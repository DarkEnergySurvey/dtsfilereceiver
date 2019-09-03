#!/usr/bin/env python
# $Id: accept_dts_delivery.py 43145 2016-07-15 15:38:09Z mgower $
# $Rev:: 43145                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-07-15 10:38:09 #$:  # Date of last commit.

""" Performs tasks that need to be performed immediately when a file is delivered by DTS """

import argparse
import datetime
import traceback
import os
import sys

import despymisc.miscutils as miscutils
import dtsfilereceiver.dts_utils as dtsutils

######################################################################
def parse_arguments(argv):
    """ Parse command line arguments """

    parser = argparse.ArgumentParser(description='Accept a file from DTS')
    parser.add_argument('--config', action='store', type=str, required=True)
    parser.add_argument('--md5sum', action='store', type=str)
    parser.add_argument('fullname', action='store')

    args = vars(parser.parse_args(argv))   # convert dict
    return args


######################################################################
def create_log_fullname(config):
    """ Create the log's full filename """

    today = datetime.datetime.now()
    datepath = "%04d/%02d" % (today.year, today.month)

    logdir = "%s/%s" % (config['accept_log_dir'], datepath)
    miscutils.coremakedirs(logdir)

    log_fullname = "%s/%04d%02d%02d_dts_accept.log" % (logdir, today.year, today.month, today.day)
    return log_fullname

######################################################################
def create_timestamp():
    """ Create a timestamp string for the current time """
    return datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")


######################################################################
def touch_file(fname):
    """ Touch a file """
    try:
        os.utime(fname, None)
    except EnvironmentError:
        open(fname, 'a').close()


######################################################################
def notify_file_delivered(fullname, config, md5sum):
    """ Create a file denoting that a file has been delivered """

    miscutils.coremakedirs(config['delivery_notice_dir'])

    print "%s - accept file - %s" % (create_timestamp(), fullname)

    notifyfile = "%s/%s.dts" % (config['delivery_notice_dir'], os.path.basename(fullname))
    print "%s - notify file - %s=%s" % (create_timestamp(), fullname, notifyfile)

    if md5sum is None:
        touch_file(notifyfile)
    else:
        with open(notifyfile, "w") as notifyfh:
            notifyfh.write("md5sum = %s\n" % md5sum)

######################################################################
def main(argv):
    """ Program entry point """

    try: # don't want any non-zero exit, want DTS to see file as delivered
        args = parse_arguments(argv)

        config = None
        if 'config' not in args:   # dts doesn't pass arguments to command
            progdir = os.path.dirname(__file__)
            args['config'] = "%s/dts_prod.cfg" % progdir
        #print "config file = ", args['config']
        config = dtsutils.read_config(args['config'])

        # redirect stdout/stderr to log file
        log_fullname = create_log_fullname(config)
        #print log_fullname
        logfh = open(log_fullname, "a")
        sys.stdout = logfh
        sys.stderr = logfh

        dts_md5sum = None
        if 'md5sum' in args:
            dts_md5sum = args['md5sum']

        notify_file_delivered(args['fullname'], config, dts_md5sum)

        logfh.close()
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=2, file=sys.stdout)

if __name__ == '__main__':
    main(sys.argv[1:])
    sys.exit(0)   # always accept file
