#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

""" Manually run the DTS post register steps """

import os
import sys
import argparse

import intgutils.wclutils as wclutils
import despymisc.miscutils as miscutils
import filemgmt.utils as fmutils
import dtsfilereceiver.dts_utils as dtsutils

#manifest_SN-X3enqueuedon2014-01-2700:54:04ZbySupernovaDeadmanTactician.json


def run_post_steps(filelist, config, fmobj):
    """ Performs steps necessary for each file """

    #print config.keys()
    firstname = miscutils.parse_fullname(filelist[0], miscutils.CU_PARSE_FILENAME)
    filetype = dtsutils.determine_filetype(firstname)
    miscutils.fwdebug(3, "DTSFILEHANDLER_DEBUG", "filetype = %s" % filetype)

    # dynamically load class specific to filetype
    classkey = 'dts_filetype_class_' + filetype
    filetype_class = miscutils.dynamically_load_class(config[classkey])
    valdict = fmutils.get_config_vals({}, config, filetype_class.requested_config_vals())
    ftobj = filetype_class(dbh=fmobj, config=valdict)

    for fullname in filelist:
        filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)
        miscutils.fwdebug(3, "DTSFILEHANDLER_DEBUG", "filename = %s" % filename)

        if dtsutils.check_already_registered(filename, fmobj):
            ftobj.post_steps(fullname)  # e.g., Rasicam

            # if success
            fmobj.commit()
        else:
            print "File must already be registered in order to run post_steps"



###########################################################################
def parse_cmdline(argv):
    """ Parse command line and return dictionary of values """

    parser = argparse.ArgumentParser(description='Run poststeps on files same as DTS')
    parser.add_argument('--config', action='store', required=True)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--path', action='store', help='path containing files of same filetype')
    group.add_argument('--file', action='store', help='single full filename')

    args = vars(parser.parse_args(argv))   # convert to dict
    return args


###########################################################################
def get_list_filenames(path):
    """ create a list of files in given path """

    if not os.path.exists(path):
        miscutils.fwdie("Error:   could not find path:  %s" % path, 1)

    filelist = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        for fname in filenames:
            filelist.append(dirpath+'/'+fname)

    return filelist


###########################################################################
def main():
    """ Perform steps """
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)  # turn off buffering of stdout

    args = parse_cmdline(sys.argv[1:])

    config = None
    with open(args['config'], 'r') as cfgfh:
        config = wclutils.read_wcl(cfgfh)

    filemgmt_class = miscutils.dynamically_load_class(config['classmgmt'])
    #valdict = fmutils.get_config_vals({}, config, filemgmt_class.requested_config_vals())
    fmobj = filemgmt_class(config=config)

    filelist = None
    if args['file'] is not None:
        filelist = [args['file']]
    else:
        filelist = get_list_filenames(args['path'])

    run_post_steps(filelist, config, fmobj)


###########################################################################
if __name__ == '__main__':
    main()
