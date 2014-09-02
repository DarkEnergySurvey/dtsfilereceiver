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
import cx_Oracle
from collections import defaultdict

import coreutils.desdbi


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
        self.debug = 0


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
        """ reads json manifest file and ingest into the DB tables EXPOSURES_IN_MANIFEST and SN_SUBMIT_REQUEST values needed to 
            determine arrival of exposures taken for a SN field."""
        allMandatoryExposureKeys = ['expid','object','date','acttime','filter']
        allExposures = self.read_json_single(fullname, allMandatoryExposureKeys, self.debug)
        self.ingestAllExposures(allExposures, self.dbh, self.debug)


    ###################################################################### 
    def read_json_single(self, json_file,allMandatoryExposureKeys, debug):
    
        coremisc.fwdebug(3, 'DTSSNMANIFEST_DEBUG', "reading file %s" % json_file)

        allExposures = []
            
        my_header = {}
        numseq = {}
        all_exposures = dict()
        with open(json_file) as my_json:
            for line in my_json:
                all_data = json.loads(line)
    
                for key, value in all_data.items():
                    errorFlag = 0
                    if key == 'header':
                        #read the values for the header (date and set_type are here)
                        my_head = value
    
                        allExposures.append(str(my_head['set_type']))
                        allExposures.append(str(my_head['createdAt']))
                        
                    if key == 'exposures':
                        #read all the exposures that were taken for the set_type in header
                        my_header =  value
    
                        numseq = my_header[0]['sequence']
                        
                        #Total Number of exposures in manifest file 
                        tot_exposures = numseq['seqtot']
                        
                        if tot_exposures is None or tot_exposures == 0:    
                            raise Exception("0 SN exposures parsed from json file")

                        for i in range(tot_exposures):
                            numseq = my_header[i]['sequence']
                            mytime = my_header[i]['acttime']
                            if mytime > 10 and numseq['seqnum'] == 2:
                                first_expnum = my_header[i]['expid']
                            
                            try:
                                for mandatoryExposureKey in (allMandatoryExposureKeys):
                                    coremisc.fwdebug(3, 'DTSSNMANIFEST_DEBUG', "mandatory key %s" % mandatoryExposureKey)
                                    key = str(mandatoryExposureKey)
                                    
                                    if my_header[i][mandatoryExposureKey]:
                                        coremisc.fwdebug(3, 'DTSSNMANIFEST_DEBUG', "mandatory key '%s' found %s" % (mandatoryExposureKey, my_header[i][mandatoryExposureKey]))
                                        coremisc.fwdebug(6, 'DTSSNMANIFEST_DEBUG', "allExposures in for: %s" % allExposures)
                                        try:
                                            if i == 0:
                                                if key == 'acttime':
                                                    key = 'EXPTIME'
                                                    all_exposures[key] = [my_header[i][mandatoryExposureKey]]
                                                elif key == 'filter':
                                                    key = 'BAND'
                                                    all_exposures[key] = [str(my_header[i][mandatoryExposureKey])]
                                                elif key == 'expid':
                                                    key = 'EXPID'
                                                    all_exposures[key] = [my_header[i][mandatoryExposureKey]]
                                                else:
                                                    all_exposures[key] = [my_header[i][mandatoryExposureKey]]
                                            else:
                                                if key == 'acttime':
                                                    key = 'EXPTIME'
                                                    all_exposures[key].append(my_header[i][mandatoryExposureKey])
                                                elif key == 'filter':
                                                    key = 'BAND'
                                                    all_exposures[key].append(str(my_header[i][mandatoryExposureKey]))
                                                elif key == 'expid':
                                                    key = 'EXPID'
                                                    all_exposures[key].append(my_header[i][mandatoryExposureKey])
                                                else:
                                                    all_exposures[key].append(my_header[i][mandatoryExposureKey])
                                                
                                        except KeyError:
                                            all_exposures[key] = [my_header[i][mandatoryExposureKey]]
    
    
                            except KeyError:
                                coremisc.fwdebug(0, 'DTSSNMANIFEST_DEBUG', "keyError: missing key %s in json entity: %s " % (mandatoryExposureKey,line))
                                errorFlag = 1
                                raise
                        
                        timestamp = all_exposures['date'][0]
                        nite = dtsutils.convert_UTCstr_to_nite(timestamp)
            
                        # get field by parsing set_type
                        #print 'xxxx', my_head['set_type']
                        myfield = my_head['set_type']
                        if len(myfield) > 5:
                            newfield = myfield[:5]
                        else: 
                            newfield = myfield

                        if not newfield.startswith('SN-'):
                            raise ValueError("Invalid field (%s).  set_type = '%s'" % (newfield, my_head['set_type']))

                        #if json_file contains a path or compression extension, then cut it to only the filename
                        jsonFile = coremisc.parse_fullname(json_file, coremisc.CU_PARSE_FILENAME)
                        
                        if tot_exposures is None or tot_exposures == 0:    
                            raise Exception("0 SN exposures parsed from json file")

                        for i in range(tot_exposures):
                            if i == 0:
                                #all_exposures['FIELD'] = [str(my_head['set_type'])]
                                all_exposures['FIELD'] = [newfield]
                                all_exposures['CREATEDAT'] = [str(my_head['createdAt'])]                
                                all_exposures['MANIFEST_FILENAME'] = [jsonFile]
                                all_exposures['NITE'] = [nite]
                                all_exposures['SEQNUM'] = [1]
                            else:
                                #all_exposures['FIELD'].append(str(my_head['set_type']))
                                all_exposures['FIELD'].append(newfield)
                                all_exposures['CREATEDAT'].append(str(my_head['createdAt']))                
                                all_exposures['MANIFEST_FILENAME'].append(jsonFile)
                                all_exposures['NITE'].append(nite)
                                all_exposures['SEQNUM'].append(1)
        
        # Add the manifest filename value in the dictionary
        #all_exposures['MANIFEST_FILENAME'] = json_file
        coremisc.fwdebug(6, 'DTSSNMANIFEST_DEBUG', "allExposures " % (all_exposures))
        
        return all_exposures
    

    ###################################################################### 
    def insert_dictionary_2Db(self, dbh, query, dictionary,debug=None):
        """Execute a query and return a cursor to a query
        :param query: string with query statement
        :param dictionary: dictionary to use in query
        :param debug: verbosity
    
        """
    
        try:      
            cur = dbh.cursor()        
            cur.execute(query,dictionary)
            coremisc.fwdebug(6, 'DTSSNMANIFEST_DEBUG', "dictionary into database " % (dictionary))
            success = 1
        #except cx_Oracle.IntegrityError as e:
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 955:
                print('Table already exists')
            elif error.code == 1031:
                print("Insufficient privileges")
            print(error.code)
            print(error.message)
            print(error.context)
            success = 0
            raise
        return success

    ###################################################################### 
    def ingestAllExposures(self, allExposures,dbh, debug=None):
        """
        Ingest all the exposures in EXPOSURES_IN_MANIFEST and SN_SUBMIT_REQUEST
    
        EXPOSURES_IN_MANIFEST
        Name                       Null?    Type
        ----------------------------------------- -------- ----------------------------
        EXPOSURE_FILENAME               NOT NULL VARCHAR2(50)
        MANIFEST_FILENAME               NOT NULL VARCHAR2(50)
        FIELD                           NOT NULL VARCHAR2(20)
        BAND                           NOT NULL VARCHAR2(5)
        EXPTIME                       NOT NULL BINARY_FLOAT
    
        SN_SUBMIT_REQUEST
        Name                       Null?    Type
        ----------------------------------------- -------- ----------------------------
        FIELD                           NOT NULL VARCHAR2(20)
        NITE                           NOT NULL VARCHAR2(8)
        BAND                           NOT NULL VARCHAR2(5)
        MANIFEST_FILENAME              NOT NULL VARCHAR2(50)
        FIRST_EXPNUM                   NOT NULL NUMBER(10)
        SEQNUM                        NOT NULL NUMBER(2)  
     
        #If SEQNUM is > 1, then it means the same field was taken again during the same night.
        #This will only happens in rare occasion when the sequence had to be aborted before it finished.
    
    
        :param allExposures: Dictionary with the following keys:
        [set_type,createdAt,expid,object,date,acttime,filter]
    
        """
    
    
        for i,exp in enumerate(allExposures['EXPID']):
            exp = str(exp)
            if len(exp) == 6:
                exposurename = 'DECam_00' + exp + '.fits'
            elif len(exp) == 7:
                exposurename = 'DECam_0' + exp + '.fits'
            elif len(exp) == 8:
                exposurename = 'DECam_' + exp + '.fits'
            if i == 0:
                allExposures['EXPOSURE_FILENAME'] = [exposurename]
            else:
                allExposures['EXPOSURE_FILENAME'].append(exposurename)
            
        newdictionary = {}
        for key in ['EXPOSURE_FILENAME','MANIFEST_FILENAME','FIELD','BAND','EXPTIME']:
            newdictionary[key] = allExposures[key]
    
        #print "xx",allExposures
        dict2Ingest = {}    
        for i in range(len(allExposures['EXPTIME'])):
            for key in newdictionary.keys():
                keytoingest = key
                valuetoingest = newdictionary[key][i]
                dict2Ingest[keytoingest] = valuetoingest
            coremisc.fwdebug(6, 'DTSSNMANIFEST_DEBUG', "dict2Ingest %s " % (dict2Ingest))
            try:
                sqlInsertExposuresInManifest = """insert into EXPOSURES_IN_MANIFEST (EXPOSURE_FILENAME,MANIFEST_FILENAME,FIELD,BAND,EXPTIME) VALUES 
                                    (:EXPOSURE_FILENAME, :MANIFEST_FILENAME, :FIELD, :BAND, :EXPTIME)""" 
            
    
                coremisc.fwdebug(3, 'DTSSNMANIFEST_DEBUG', "sqlInsertExposuresInManifest %s " % (sqlInsertExposuresInManifest))
                success = self.insert_dictionary_2Db(dbh, sqlInsertExposuresInManifest, dict2Ingest, debug=debug)
            
                if success:
                    coremisc.fwdebug(1, 'DTSSNMANIFEST_DEBUG', "Insert into EXPOSURES_IN_MANIFEST was successful..")
                
            except cx_Oracle.IntegrityError as e:
                print "error while inserting into EXPOSURES_IN_MANIFEST: ", e 
                raise
    

        ########################################################################################
        #
        #Fix first expnum. First expnum is the first exposure for each filter set. In case of
        #one a field with one filter exposure, then first_expnum = expnum.
        #For more than one exposure / band/field, then first_expnum = first exposure of set.
        #
    
        #Determine index of list for exptime = 10. (poiting exposure)
        allexps=  allExposures['EXPTIME']
        coremisc.fwdebug(6, 'DTSSNMANIFEST_DEBUG', "all exptimes %s" % (allexps))
        for i,val in enumerate(allexps):
            if val == 10.0:
                pointingIndex = i
            
        coremisc.fwdebug(3, 'DTSSNMANIFEST_DEBUG', "pointing Exposure index is %s" % pointingIndex)

        #find where there are repetead bands, but exclude the band where the exptime = 10
        ListofBands = allExposures['BAND']
        coremisc.fwdebug(3, 'DTSSNMANIFEST_DEBUG', "listOfaBands...%s" % ListofBands)

        bandsDicIndexes = defaultdict(list)

        for i,item in enumerate(allExposures['BAND']):
            bandsDicIndexes[item].append(i)
    
        #I have to loop trorugh the dictionary for all the bands. Cound how many bands. Get the vaues from this dictionary
        #which is the index to the list, and use that to determine the elementes for all the other dictionaries.
        #I need the follwoing elementsl 'FIELD','NITE','BAND','MANIFEST_FILENAME','FIRST_EXPNUM','SEQNUM'
        ind2use = []
        flag_first = 0
        for ind, b in enumerate(ListofBands):
            coremisc.fwdebug(6, 'DTSSNMANIFEST_DEBUG', "indexes %s %s" % (bandsDicIndexes[b], ind))
            if ind == pointingIndex:
                coremisc.fwdebug(3, 'DTSSNMANIFEST_DEBUG', "found pointing index %s %s " % (ind, pointingIndex))
                continue
            else:
                #for two exposures and one of them is the poiting
                if len(bandsDicIndexes[b]) <= 2 and ind == pointingIndex+1:
                    ind2use.append((max(bandsDicIndexes[b])))
                    #print "the index", ind2use
                #if there are more than 2 exposures (generally for deep fields
                elif len(bandsDicIndexes[b]) > 2 and ind == pointingIndex+1:
                    ind2use.append(bandsDicIndexes[b][ind])
                    flag_first = 1
                elif len(bandsDicIndexes[b]) == 1:
                    ind2use.append(bandsDicIndexes[b][0])
                elif len(bandsDicIndexes[b]) == 2 and ind != pointingIndex and flag_first==0:
                    ind2use.append(min(bandsDicIndexes[b]))
                    flag_first = 1
            if flag_first:
                break
    
        #contruct the dictionary with only the elements that needs to go into the DB
        #To do this use the ind2use extracted from the above list.
        newDic = {}
        for index in ind2use:
            #print index
            newDic['FIELD'] = allExposures['FIELD'][index]
            newDic['NITE'] = allExposures['NITE'][index]
            newDic['BAND'] = allExposures['BAND'][index]
            newDic['MANIFEST_FILENAME'] = allExposures['MANIFEST_FILENAME'][index]
            newDic['FIRST_EXPNUM'] = allExposures['EXPID'][index]
            newDic['SEQNUM'] = allExposures['SEQNUM'][index]
            coremisc.fwdebug(6, 'DTSSNMANIFEST_DEBUG', "index=%s, newDic=%s" % (index, newDic))
        
            #Ingest into the database each of them
            try:
                sqlInsertSNSubmitRequest = """insert into SN_SUBMIT_REQUEST (FIELD,NITE,BAND,MANIFEST_FILENAME,FIRST_EXPNUM,SEQNUM) VALUES 
                                            (:FIELD, :NITE, :BAND, :MANIFEST_FILENAME, :FIRST_EXPNUM, :SEQNUM)""" 
        
                coremisc.fwdebug(3, 'DTSSNMANIFEST_DEBUG', "sqlInsertSNSubmitRequest = %s" % sqlInsertExposuresInManifest)

                success = self.insert_dictionary_2Db(dbh,sqlInsertSNSubmitRequest, newDic,debug=debug)
                if success:
                    coremisc.fwdebug(1, 'DTSSNMANIFEST_DEBUG', "Insert into SN_SUBMIT_REQUEST was successful..")
        
            except cx_Oracle.IntegrityError as e:
                print "error while inserting into SN_SUBMIT_REQUEST: ", e 
                raise

