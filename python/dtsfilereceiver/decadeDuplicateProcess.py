from dtsfilereceiver.decadeFileBase import DecadeFileBase

import datetime
import os
import glob


class LogRecord(object):
    def __init__(self, text):
        self.filename = None
        self.path = None
        self.error = False
        self.success = False
        self.duplicate = False
        self.errormsg = []
        self.newname = None
        for line in text:
            if 'handle_file' in line:
                if 'filename =' in line:
                    temp = line.split()[-1]
                    loc = temp.rfind('/')
                    self.path = temp[:loc]
                    self.filename = temp[loc+1:] + '.fz'
                elif 'success' in line and 'committing' in line:
                    self.success = True
            elif 'Error' in line:
                self.error = True
                self.errormsg.append(line)
                if 'unique constraint' in line and 'DESFILE_TYPE_NAME_COMPRESS_BTX' in line:
                    self.duplicate = True
            elif 'Duplicate file' in line:
                self.error = True
                self.duplicate = True

class DecadeDuplicateProcess(DecadeFileBase):
    def __init__(self, config, logfile):
        DecadeFileBase.__init__(self, config, 'dup')
        self.logfile = open(logfile, 'r')
        self.logger.info('Processing %s' % self.logfile)
        now = datetime.datetime.now()
        self.basedir = os.path.join(self.config['bad_file_dir'],str(now.year),str(now.month))
        self.files = []
        self.starttime = datetime.datetime.now()
        self.number_ingested = 0
        self.number_duplicates = 0
        self.errors = 0

    def process_log(self):
        self.records = []
        self.duplicates = []
        self.other = []
        rl = self.logfile.readlines()
        current = []
        for line in rl:
            line = line.strip()
            if 'main - ===============' in line:
                self.records.append(LogRecord(current))
                current = []
            else:
                current.append(line)
        for rec in self.records:
            if rec.error:
                if rec.duplicate:
                    self.duplicates.append(rec)
                else:
                    self.other.append(rec)
            else:
                self.number_ingested += 1
        self.logger.info('Found %i errors (%i duplicates)' % (len(self.duplicates) + len(self.other), len(self.duplicates)))
        self.number_duplicates = len(self.duplicates)
        self.errors = len(self.other)
        if not self.duplicates:
            self.logger.info('No duplicates found')
        else:
            badfiles = glob.glob(os.path.join(self.config['bad_file_dir'],'*','*','DECam*'))
            delfiles = []
            dbdelfiles = []

            for rec in self.duplicates:
                for bf in badfiles:
                    if rec.filename in bf:
                        delfiles.append(bf)
                        dbdelfiles .append((rec.filename,))
            for fn in delfiles:
                os.remove(os.path.join(self.config['bad_file_dir'],fn))
            self.logger.info('Deleted %i duplicates from disk' % len(delfiles))
            #print len(dbdelfiles)
            #print dbdelfiles
            self.delete_temp_files_from_db(dbdelfiles)
        if not self.other:
            self.logger.info('No bad files found')
        else:
            self.logger.info('Bad files:')
            for rec in self.other:
                self.logger.info('  %s' % rec.filename)
                for msg in rec.errormsg:
                    self.logger.info('    %s' % msg)

    def report(self):
        self.basic_insert_row('INGEST_MONITOR', {'run_date': self.starttime, 'number_ingested':self.number_ingested,'duplicates':self.number_duplicates,'errors':self.errors})
