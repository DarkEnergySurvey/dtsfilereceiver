#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

""" Performs tasks that need to be performed immediately when a file is delivered by DTS """


import argparse
import datetime
import traceback
import re
import os
import sys


def parse_arguments(argv):

    parser = argparse.ArgumentParser(description='Accept a file from DTS')
    parser.add_argument('--config', action='store', type=str)
    parser.add_argument('fullname', nargs=1, action='store')

    args = vars(parser.parse_args(argv))   # convert dict
    return args


def makedirs(thedir):
    if len(thedir) > 0 and not os.path.exists(thedir):  # some parallel filesystems really don't like
                                                        # trying to make directory if it already exists
        try:
            os.makedirs(thedir)
        except OSError as exc:      # go ahead and check for race condition
            if exc.errno == errno.EEXIST:
                pass
            else:
                raise

# FWRefact +7

def create_log_fullname(config):
    today = datetime.datetime.now()
    datepath = "%04d/%02d" % (today.year, today.month)

    logdir = "%s/%s" % (config['accept_log_dir'], datepath)
    makedirs(logdir) 

    log_fullname = "%s/%04d%02d%02d_dts_accept.log" % (logdir, today.year, today.month, today.day)
    return log_fullname


def read_config(cfgfile):
    config = {}
    cnt = 0
    with open(cfgfile, "r") as cfgfh:
        for line in cfgfh:
            cnt += 1
            line = line.strip() 
            if len(line) > 0 and not line.startswith('#'):
                m = re.match("([^=]+)\s*=\s*(.*)$", line)
                config[m.group(1).strip()] = m.group(2).strip()

    return config
        
    

def create_timestamp():
    return datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")


def touch_file(fname):
    """ Touch a file """
    try:
        os.utime(fname, None)
    except:
        open(fname, 'a').close()


def notify_file_delivered(fullname, config):
    """  """

    makedirs(config['delivery_notice_dir'])

    print "%s - accept file - %s" % (create_timestamp(), fullname)

    notifyfile = "%s/%s.dts" % (config['delivery_notice_dir'], os.path.basename(fullname))
    print "%s - notify file - %s=%s" % (create_timestamp(), fullname, notifyfile)
    touch_file(notifyfile)


if __name__ == '__main__':
    try: # don't want any non-zero exit, want DTS to see file as delivered
        args = parse_arguments(sys.argv[1:])

        config = None
        if 'config' not in args:   # dts doesn't pass arguments to command
            progdir = os.path.dirname(__file__)
            args['config'] = "%s/dts_prod.cfg" % progdir
        print "config file = ", args['config']
        config = read_config(args['config'])

        # redirect stdout/stderr to log file 
        log_fullname = create_log_fullname(config)
        print log_fullname
        logfh = open(log_fullname, "a")
        sys.stdout = logfh
        sys.stderr = logfh

        notify_file_delivered(args['fullname'][0], config)

        logfh.close()
    except: 
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=2, file=sys.stdout)

    sys.exit(0)   # always accept file
