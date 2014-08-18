#!/usr/bin/env python

import commands
import os
import time
import sys
import re
import argparse
import shutil # move file
import traceback
from datetime import datetime, timedelta


import intgutils.wclutils as wclutils
import coreutils.desdbi as desdbi
import coreutils.miscutils as coremisc
import filemgmt.utils as fmutils
import dtsfilehandler.dtsutils as dtsutils

#manifest_SN-X3enqueuedon2014-01-2700:54:04ZbySupernovaDeadmanTactician.json


def run_post_steps(fullname, config, filemgmt):
    """ Performs steps necessary for each file """

    print config.keys()

    filename = coremisc.parse_fullname(fullname, coremisc.CU_PARSE_FILENAME)
    coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "filename = %s" % filename)

    if not check_already_registered(filename, filemgmt):
        filetype = determine_filetype(filename, config)
        coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "filetype = %s" % filetype)

        # dynamically load class specific to filetype
        classkey = 'dts_filetype_class_' + filetype
        filetype_class = coremisc.dynamically_load_class(config[classkey]) 
        valDict = fmutils.get_config_vals({}, config, filetype_class.requested_config_vals())
        filetypeObj = filetype_class(dbh=filemgmt, config=valDict)

        filetypeObj.post_steps(location_info['fullname'])  # e.g., Rasicam
        filemgmt.commit()
    else:
        print "File must already be registered in order to run post_steps"

                            

###########################################################################
def parse_cmdline(argv):
    """ Parse command line and return dictionary of values """

    parser = argparse.ArgumentParser(description='Handle files delivered by DTS') 
    parser.add_argument('--config', action='store', required=True)
    parser.add_argument('fullname', action='store')

    args = vars(parser.parse_args(argv))   # convert to dict
    return args


###########################################################################
if __name__ == '__main__':
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)  # turn off buffering of stdout

    args = parse_cmdline(sys.argv[1:])
    print args

    config = None
    with open(args['config'], 'r') as fh:
        config = wclutils.read_wcl(fh)

    filemgmt_class = coremisc.dynamically_load_class(config['classmgmt'])
    #valDict = fmutils.get_config_vals({}, config, filemgmt_class.requested_config_vals())
    filemgmt = filemgmt_class(config=config)

       handle_file(fpair[0], fpair[1], config, filemgmt, task_id)

