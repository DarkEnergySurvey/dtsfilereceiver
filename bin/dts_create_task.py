#!/usr/bin/env python
# $Id: dts_create_task.py 43145 2016-07-15 15:38:09Z mgower $
# $Rev:: 43145                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2016-07-15 10:38:09 #$:  # Date of last commit.

""" Creates and begins task against which dts file provenance will be tracked """

import argparse
import sys
import despydmdb.desdmdbi as desdmdbi

###########################################################################
def parse_cmdline(argv):
    """ Parse command line and return dictionary of values """

    parser = argparse.ArgumentParser(description='Create DTS task')
    parser.add_argument('--label', action='store', required=True)
    parser.add_argument('--des_services', action='store')
    parser.add_argument('--des_db_section', '--section', action='store')

    args = vars(parser.parse_args(argv))   # convert to dict
    return args

###########################################################################
def main(argv):
    """ Program entry point """

    args = parse_cmdline(argv)
    dbh = desdmdbi.DesDmDbi(args['des_services'], args['des_db_section'])

    print "Creating task with name='dts' and label='%s'" % args['label']
    task_id = dbh.create_task(name='dts', info_table=None, parent_task_id=None,
                              root_task_id=None, i_am_root=True, label=args['label'],
                              do_begin=True, do_commit=True)
    row = {'task_id': task_id, 'prov_msg': 'dts file receiver %s'% args['label']}
    dbh.basic_insert_row('FILE_REGISTRATION', row)
    dbh.commit()
    dbh.close()

    print "Update the DTS config file:   dts_task_id = %d" % task_id

if __name__ == "__main__":
    main(sys.argv[1:])
