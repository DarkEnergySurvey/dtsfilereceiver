# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
"""

__version__ = "$Rev$"

import os
import shutil
import copy
from collections import OrderedDict
import pyfits
from datetime import datetime

import despymisc.miscutils as miscutils
import intgutils.metautils as metautils
import wrappers.WrapperUtils as wraputils
import wrappers.WrapperFuncs as wrapfuncs

class DTSraw():
    """
    """
    # assumes home, target, and job dirs are read/write same machine

    @staticmethod
    def requested_config_vals():
        return {'keywords_file':'REQ', 'filetype_metadata':'OPT', 'raw_project':'REQ'}

    ###################################################################### 
    def __init__(self, dbh, config):
        self.config = config
        self.dbh = dbh
        self.verbose = 0

        keyfile = self.config['keywords_file']

        self.keywords = {'pri':{}, 'ext':{}}
        with open(keyfile, 'r') as fh:
            for line in fh:
                line = line.upper()
                
                (keyname, pri, ext) = miscutils.fwsplit(line, ',')
                if pri != 'Y' and pri != 'N' and pri != 'R':
                    raise ValueError('Error: Invalid primary entry in keyword file (%s)' % line)
                if ext != 'Y' and ext != 'N' and ext != 'R':
                    raise ValueError('Error: Invalid extenstion entry in keyword file (%s)' % line)
                self.keywords['pri'][keyname] = pri
                self.keywords['ext'][keyname] = ext


    ###################################################################### 
    def get_metadata(self, fullname):
        ftype = 'raw'

        filetype_metadata = self.config['filetype_metadata']
        # note:  When manually ingesting files generated externally to the framework, 
        #        we do not want to modify the files (i.e., no updating/inserting headers
        #(reqmeta, optmeta, updatemeta) = metautils.create_file_metadata_dict(ftype, filetype_metadata, None, None)
        metaspecs = metautils.get_metadata_specs(ftype, filetype_metadata, None, None, False)

        if metaspecs is None:
            raise ValueError("Error: Could not find metadata specs for filetype '%s'" % filetype)

        for key in metaspecs:
            #print "metaspecs key = ", key
            if type(metaspecs[key]) == dict or type(metaspecs[key]) == OrderedDict:
                if metautils.WCL_META_WCL in metaspecs[key]:
                    #print "deleting wcl from", key
                    del metaspecs[key][metautils.WCL_META_WCL]   # remove wcl requirements for non-pipeline file ingestion
                elif len(metaspecs[key]) == 0:
                    del metaspecs[key]
        metaspecs['filetype'] = ftype
        metaspecs['fullname'] = fullname

        #print metaspecs
        filemeta = wraputils.get_file_metadata(metaspecs)   # get the metadata from the fits files
        self.filemeta = filemeta['file_1']
        return self.filemeta



    ###################################################################### 
    def check_header_keywords(self, hdunum, hdr):
        # missing has the keywords which are missing in the file and are required for processing
        # extra are the keywords which are not required and are present in the system
        # not required are the ones which are not required and are not present

        req_missing = []
        want_missing = []
        extra = []

        hdutype = 'ext'
        if hdunum == 0:
            hdutype = 'pri'
        
        for keyw,status in self.keywords[hdutype].items():
            if keyw not in hdr:
                if status=='R':
                    req_missing.append(keyw)
                elif status=='Y':
                    want_missing.append(keyw) 


        # check for extra keywords
        for keyw in hdr:
            if keyw not in self.keywords[hdutype] or \
                self.keywords[hdutype][keyw] == 'N':
                extra.append(keyw)
    
        return (req_missing, want_missing, extra)
            


    ###################################################################### 
    def check_valid(self, fullname): # should raise exception if not valid

        # check fits file
        hdulist = pyfits.open(fullname)
        prihdr = hdulist[0].header

        instrume = prihdr['INSTRUME'].lower()

        req_num_hdus = -1
        if instrume == 'decam':
            req_num_hdus = 71
        else:
            raise ValueError('Error:  Unknown instrume (%s)' % instrume)

        # check # hdus
        num_hdus = len(hdulist)
        if num_hdus != req_num_hdus:
            raise ValueError('Error:  Invalid number of hdus (%s)' % num_hdus)

        # check keywords
        for hdunum in range(0, num_hdus):
            hdr = hdulist[hdunum].header
            (req, want, extra) = self.check_header_keywords(hdunum, hdr)

            if self.verbose > 1:
                if want is not None and len(want) > 0:
                    print "HDU #%02d Missing requested keywords: %s" % (hdunum, want)
                if extra is not None and len(extra) > 0:
                    print "HDU #%02d Extra keywords: %s" % (hdunum, extra)
                
            if req is not None and len(req) > 0:
                raise ValueError('Error: HDU #%02d Missing required keywords (%s)' % (hdunum, req))


    ###################################################################### 
    def get_archive_path(self, fullname):
        #dirpat = 'raw'
        #filepathpat = self.config[DIRPATSECT][dirpat]['OPS']
        #filepath = self.interpolate(filepathpat, searchopts)

        filepath = '%s/raw/%s' % (self.config['raw_project'], self.filemeta['nite'])
        return filepath



    ###################################################################### 
    def insert_rasicam(self, fullname):
        DBtable='rasicam_decam'

        #  Keyword list needed to update the database.
        #     i=int, f=float, b=bool, s=str, date=date
        keylist = { 'EXPNUM':'i',
                    'INSTRUME':'s', 
                    'SKYSTAT':'b',
                    'SKYUPDAT':'date',
                    'GSKYPHOT':'b',
                    'LSKYPHOT':'b',
                    'GSKYVAR':'f',
                    'GSKYHOT':'f',
                    'LSKYVAR':'f',
                    'LSKYHOT':'f',
                    'LSKYPOW':'f' }

        if (not(os.path.isfile(fullname))):
            raise Exception("Exposure not found: '%s'" % fullname)

        filename = miscutils.parse_fullname(fullname, miscutils.CU_PARSE_FILENAME)

        row = {}
        row['filename'] = filename
        row['source'] = 'HEADER'
        row['analyst'] = 'DTS.ingest'

        hdulist = pyfits.open(fullname)
        primary_hdr = hdulist[0].header

        numkey_found = 0
        for key, ktype in keylist.items():
            if (key.upper() in primary_hdr):
                numkey_found += 1
                value = primary_hdr[key]
                #print primary_hdr[key]
                if (key == 'SKYUPDAT'):  # entry_time is time exposure taken 
                    row['ENTRY_TIME'] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                elif (key == 'INSTRUME'):
                    row['CAMSYM'] = wrapfuncs.func_camsym(fullname)   
                elif (ktype == 'b'):
                    if (value):
                        row[key] = 'T'
                    else:
                        row[key] = 'F'
                elif (ktype == 'i'):
                    if value != 'NaN':
                        row[key] = int(value)
                else:
                    if value != 'NaN':
                        row[key] = float(value)

        #print "row = %s" % row

        if (numkey_found > 0):
            self.dbh.basic_insert_row(DBtable, row)
        else:
            raise Exception("No RASICAM header keywords identified for %s" % filename)
        


    ###################################################################### 
    def post_steps(self, fullname): # e.g., Rasicam
        self.insert_rasicam(fullname)
