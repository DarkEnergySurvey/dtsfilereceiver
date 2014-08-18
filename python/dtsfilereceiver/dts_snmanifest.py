# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
"""

__version__ = "$Rev$"

import coreutils.miscutils as coremisc
import dtsfilereceiver.dts_utils as dtsutils
import json

class DTSsnmanifest():
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


    ###################################################################### 
    def get_metadata(self, fullname):
        ftype = 'snmanifest'

        filename = coremisc.parse_fullname(fullname, coremisc.CU_PARSE_FILENAME)
        filemeta = {'file_1': {'filename': filename, 'filetype':ftype}}
        self.filemeta = filemeta['file_1']

        return self.filemeta



    ###################################################################### 
    def check_valid(self, fullname): # should raise exception if not valid
        pass


    ###################################################################### 
    def get_archive_path(self, fullname):
        nite = None
        with open(fullname, 'r') as jsonfh:
            line = jsonfh.readline()
            linedata = json.loads(line)
            datestr = linedata['exposures'][0]['date']
            nite = dtsutils.convert_UTCstr_to_nite(datestr)
        
        filepath = '%s/%s/%s' % (self.config['raw_project'], 
                                 self.filemeta['filetype'],
                                 nite)
        return filepath


    ###################################################################### 
    def post_steps(self, fullname): # e.g., Rasicam
        print "post_steps called"
        pass
