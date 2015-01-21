#!/usr/bin/env python

import commands
import os
import time
import sys
import re
import argparse
import shutil # move file
import traceback
import hashlib
from datetime import datetime, timedelta


import intgutils.wclutils as wclutils
import coreutils.desdbi as desdbi
import coreutils.miscutils as coremisc
import filemgmt.utils as fmutils


def read_notify_file(notify_file):
    """ Return contents of notify file as dictionary """
    notifydict = {}
    with open(notify_file, "r") as fh:
        for line in fh:
            m = re.match("^\s*(\S+)\s*=(.+)\s*$", line)
            notifydict[m.group(1).strip().lower()] = m.group(2).strip()
    return notifydict


def calc_md5sum(filename, blsize=2**20):
    """ Calculate md5sum for file """

    md5 = hashlib.md5()
    with open(filename, "rb") as fh:
        data = fh.read(blsize)
        while data:
            md5.update(data)
            data = fh.read(blsize)

    return md5.hexdigest()


def stop_if_already_running():
    """ Exits program if program is already running """

    script_name = os.path.basename(__file__)
    l = commands.getstatusoutput("ps aux | grep -e '%s' | grep -v grep | grep -v vim | awk '{print $2}'| awk '{print $2}' " % script_name)
    if l[1]:
        print "Already running.  Aborting"
        print l[1]
        sys.exit(0)


def determine_filetype(filename, config):
    """ Returns the filetype of the given file or None if cannot determine filetype """
    filetype = None

    #print filename

    if filename.endswith('.fits'):
        filetype = 'raw'
    elif filename.startswith('manifest_SN') and filename.endswith('.json'):
        filetype = 'snmanifest' 

    return filetype


def check_already_registered(filename, filemgmt):
    """ Throws exception if file is already registered """

    #print "check_already_registered: %s" % filename
    
    has_meta = filemgmt.file_has_metadata([filename])
    if len(has_meta)  == 1:
        raise Exception("%s already exists in DB" % filename) 

    #sql = "select count(*) from genfile where filename=%s" % dbh.get_named_bind_string('filename')
    #print sql
    #curs = dbh.cursor()
    #curs.execute(sql, {'filename': filename})
    #(number_of_rows,)=curs.fetchone()
    #if int(number_of_rows) == 1:
    #    raise Exception("%s already exists in DB" % filename) 
    #elif int(number_of_rows) > 1:
    #    raise Exception("Programming error: genfile query returned more than 1 result for %s" % filename)
    

def save_data_db(filemgmt, task_id, metadata, location_info, prov):
    #print "save_data_db"

    if metadata is None:
        raise Exception("Error: save_data_db metadata is None")
    if location_info is None:
        raise Exception("Error: save_data_db location_info is None")
    if  prov is None:
        raise Exception("Error: save_data_db prov is None")

    filemgmt.ingest_file_metadata(metadata)
    filemgmt.ingest_provenance(prov, {'exec_1': task_id}) 
    filemgmt.register_file_in_archive({location_info['filename']: location_info}, {'archive': config['archive_name']})


def move_file_to_archive(config, delivery_fullname, archive_rel_path, dts_md5sum):  
    """ Move file to its location in the archive """
    
    basename = os.path.basename(delivery_fullname)
    root = config['archive'][config['archive_name']]['root']
    path = "%s/%s" % (root, archive_rel_path)
    dst = "%s/%s" % (path, basename)
    print delivery_fullname, dst

    coremisc.coremakedirs(path)

    #shutil.move(delivery_fullname, dst)  replace move by cp+unlink
    max_cp_tries = 5
    cp_cnt = 1
    copied = False
    while cp_cnt <= max_cp_tries and not copied: 
        shutil.copy2(delivery_fullname, dst) # similar to cp -p

        starttime = datetime.now()
        md5_after_move = calc_md5sum(dst)
        endtime = datetime.now()
        print "%s: md5sum after move %s (%s secs)" % (delivery_fullname, md5_after_move, (endtime-starttime).total_seconds()) 

        if dts_md5sum is None: 
            copied = True
        elif dts_md5sum != md5_after_move:
            print "Warning: md5 doesn't match after cp (%s, %s)" % (delivery_fullname, dst)
            time.sleep(5)
            os.unlink(dst)   # remove bad file from archive
            cp_cnt += 1
        else:
            copied = True

    if not copied:
        raise Exception("Error: cannot cp file without md5sum problems.")

    os.unlink(delivery_fullname)

    (path, filename, compress) = coremisc.parse_fullname(dst, coremisc.CU_PARSE_PATH | \
                                                              coremisc.CU_PARSE_FILENAME | \
                                                              coremisc.CU_PARSE_EXTENSION)

    fileinfo = {'fullname': dst,
                'filename' : filename,
                'compression': compress,
                'path': path,
                'filesize': os.path.getsize(dst),
                'md5sum': md5_after_move }

    return fileinfo


def generate_provenance(fullname):
    """ Generate provenance wcl """
    (fname, compression) = coremisc.parse_fullname(fullname, coremisc.CU_PARSE_FILENAME | coremisc.CU_PARSE_EXTENSION)
    if compression is not None:
        fname += compression
    prov = {'was_generated_by': {'exec_1': fname}}    # includes compression extension
    return prov


def handle_file(notify_file, delivery_fullname, config, filemgmt, task_id):
    """ Performs steps necessary for each file """

    filetype = None
    metadata = None
    location_info = None
    prov = None

    # read values from notify file
    notifydict = read_notify_file(notify_file)

    # use dts_md5sum from notify_file
    dts_md5sum = None
    if 'md5sum' in notifydict:
        dts_md5sum = notifydict['md5sum']

    print "%s: dts md5sum = %s" % (delivery_fullname, dts_md5sum)

    #print config.keys()
    try: 
        filename = coremisc.parse_fullname(delivery_fullname, coremisc.CU_PARSE_FILENAME)
        coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "filename = %s" % filename)

        if not os.path.exists(delivery_fullname):
            print "Warning:  delivered file does not exist:"
            print "\tnotification file: %s" % notify_file
            print "\tdelivered file: %s" % delivery_fullname
            print "\tRemoving notification file and continuing"
            os.unlink(notify_file)
            return
            
        if dts_md5sum is not None:
            starttime = datetime.now()
            md5_before_move = calc_md5sum(delivery_fullname)
            endtime = datetime.now()
            print "%s: md5sum before move %s (%s secs)" % (delivery_fullname, md5_before_move, (endtime-starttime).total_seconds()) 
            if md5_before_move != dts_md5sum:
                print "%s: dts md5sum = %s" % (delivery_fullname, dts_md5sum)
                print "%s: py  md5sum = %s" % (delivery_fullname, md5_before_move)
                raise Exception("Error: md5sum in delivery dir not the same as DTS-provided md5sum")

        if not check_already_registered(filename, filemgmt):
            filetype = determine_filetype(filename, config)
            coremisc.fwdebug(3, "DTSFILEHANDLER_DEBUG", "filetype = %s" % filetype)

            # dynamically load class specific to filetype
            classkey = 'dts_filetype_class_' + filetype
            filetype_class = coremisc.dynamically_load_class(config[classkey]) 
            valDict = fmutils.get_config_vals({}, config, filetype_class.requested_config_vals())
            filetypeObj = filetype_class(dbh=filemgmt, config=valDict)

            metadata = filetypeObj.get_metadata(delivery_fullname)
            metadata['filename'] = filename 
            metadata['filetype'] = filetype
            coremisc.fwdebug(3, "DTSFILEHANDLER_DEBUG", 'len(metadata) = %s' % len(metadata))
            coremisc.fwdebug(6, "DTSFILEHANDLER_DEBUG", 'metadata = %s' % metadata)

            filetypeObj.check_valid(delivery_fullname)  # should raise exception if not valid
            archive_rel_path = filetypeObj.get_archive_path(delivery_fullname)
            prov = generate_provenance(delivery_fullname)

            coremisc.fwdebug(3, "DTSFILEHANDLER_DEBUG", 'archive_rel_path = %s' % archive_rel_path)
            coremisc.fwdebug(3, "DTSFILEHANDLER_DEBUG", 'prov = %s' % prov)

            location_info = move_file_to_archive(config, delivery_fullname, archive_rel_path, dts_md5sum)

            save_data_db(filemgmt, task_id, {'file_1': metadata}, location_info, prov)

            filetypeObj.post_steps(location_info['fullname'])  # e.g., Rasicam

            # if success
            filemgmt.commit()
            os.unlink(notify_file)
        else:
            handle_bad_file(config, notify_file, delivery_fullname, filemgmt, 
                            filetype, metadata, location_info, prov, 
                            "already registered")
    except Exception as err:
        (type, value, trback) = sys.exc_info()
        print "******************************"
        print "Error: %s" % delivery_fullname
        traceback.print_exception(type, value, trback, file=sys.stdout)
        print "******************************"

        handle_bad_file(config, notify_file, delivery_fullname, filemgmt, 
                        filetype, metadata, location_info, prov, 
                        "Exception: %s" % err)
    except SystemExit:   # Wrappers code calls exit if cannot find header value
        handle_bad_file(config, notify_file, delivery_fullname, filemgmt, 
                        filetype, metadata, location_info, prov, 
                        "SystemExit: Probably missing header value.  Check log for error msg.")
        
    filemgmt.commit()



def handle_bad_file(config, notify_file, delivery_fullname, dbh, 
                    filetype, metadata, location_info, prov, msg):
    """ Perform steps required by any bad file """

    dbh.rollback()  # undo any db changes for this file

    coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "delivery_fullname = %s" % delivery_fullname)
    coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "filetype = %s" % filetype)
    coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "msg = %s" % msg)
    coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "metadata = %s" % metadata)
    coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "location_info = %s" % location_info)
    coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "prov = %s" % prov)

    today = datetime.now()
    datepath = "%04d/%02d" % (today.year, today.month)

    # where is file now
    if location_info is None:
        orig_fullname = delivery_fullname
    else:
        orig_fullname = location_info['fullname']

    # create a uniq name for living in the "bad file" area
    # contains relative path for storing in DB
    uniq_fullname = "%s/%s.%s" % (datepath, os.path.basename(orig_fullname), 
                                  today.strftime("%Y%m%d%H%M%S%f")[:-3])

    # absolute path
    destbad = "%s/%s" % (config['bad_file_dir'], uniq_fullname)

    if os.path.exists(destbad):
        coremisc.fwdebug(0, "DTSFILEHANDLER_DEBUG", "WARNING: bad file already exists (%s)" % destbad)
        os.remove(destbad)
    
    # make directory in "bad file" area and move file there
    coremisc.coremakedirs(os.path.dirname(destbad))
    shutil.move(orig_fullname, destbad) 

    # save information in db about bad file
    row = {}

    # save extra metadata if it exists
    if metadata is not None:
        badcols = dbh.get_column_names('DTS_BAD_FILE')

        for c in badcols:
            if c in metadata:
                row[c] = metadata[c]

    row['task_id'] = config['dts_task_id']
    t = os.path.getmtime(notify_file)
    row['delivery_date'] = datetime.fromtimestamp(t)
    row['orig_filename'] = os.path.basename(orig_fullname)
    row['uniq_fullname'] = uniq_fullname
    row['rejected_date'] = today
    row['rejected_msg'] = msg
    row['filesize'] = os.path.getsize(destbad)
    if filetype is not None:
        row['filetype'] = filetype


    dbh.basic_insert_row('DTS_BAD_FILE', row)
    dbh.commit()
    os.unlink(notify_file)
    


###########################################################################
def parse_cmdline(argv):
    """ Parse command line and return dictionary of values """

    parser = argparse.ArgumentParser(description='Handle files delivered by DTS') 
    parser.add_argument('--config', action='store', required=True)
    parser.add_argument('--poststepOnly', action='store', 
                        help='Only run poststeps on given file.  Must already be registered.')
    #parser.add_argument('fullname', action='store')
    #parser.add_argument('--classmgmt', action='store')
    #parser.add_argument('--classutils', action='store')
    #parser.add_argument('--des_services', action='store')
    #parser.add_argument('--des_db_section', action='store')
    #parser.add_argument('--archive', action='store', help='single value')
    #parser.add_argument('--verbose', action='store', default=1)
    #parser.add_argument('--version', action='store_true', default=False)

    args = vars(parser.parse_args(argv))   # convert to dict
    return args


###########################################################################
def get_list_files(notify_dir, delivery_dir):
    filenames = next(os.walk(notify_dir))[2]

    delivery_filenames = []
    for f in filenames:
        #print f
        nf = "%s/%s" % (notify_dir, f)
        df = "%s/%s" % (delivery_dir, re.sub('.dts$','',f))
        delivery_filenames.append([nf, df])

    return delivery_filenames

        


###########################################################################
if __name__ == '__main__':
    stop_if_already_running()

    #print "sleeping"
    #time.sleep(3000)

    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)  # turn off buffering of stdout
    args = parse_cmdline(sys.argv[1:])
    #print args

    config = None
    with open(args['config'], 'r') as fh:
        config = wclutils.read_wcl(fh)

    filepairs = get_list_files(config['delivery_notice_dir'], config['delivery_dir'])
    #print filepairs

    if (len(filepairs) > 0):
        #dbh = desdbi.DesDbi(config['des_services'],config['des_db_section'])
        filemgmt = None

        filemgmt_class = coremisc.dynamically_load_class(config['classmgmt'])
        #valDict = fmutils.get_config_vals({}, config, filemgmt_class.requested_config_vals())
        filemgmt = filemgmt_class(config=config)
        config['filetype_metadata'] = filemgmt.get_all_filetype_metadata()
        config['archive'] = filemgmt.get_archive_info()

        task_id = config['dts_task_id']  # get task id for dts

        for fpair in filepairs:
            handle_file(fpair[0], fpair[1], config, filemgmt, task_id)
    else:
        print "0 files to handle"

