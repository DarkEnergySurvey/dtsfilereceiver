""" Utility classes and functions for decadOAe file processing
"""

import os
import logging
from logging.handlers import TimedRotatingFileHandler

import despydmdb.desdmdbi as desdmdbi
import dtsfilereceiver.dts_utils as dtsutils



class DecadeFileBase(desdmdbi.DesDmDbi):
    """ Base class
    """
    def __init__(self, config, logref='dft'):
        self.config = dtsutils.read_config(config)
        desdmdbi.DesDmDbi.__init__(self, self.config['des_services'], self.config['des_db_section'])
        if 'logger' in self.config:
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(logref)
            handler = TimedRotatingFileHandler(os.path.join(self.config['log_root'], self.config[logref + '_log']), when="midnight")
            handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)s  %(message)s", datefmt='%Y-%m-%d %H:%M:%S'))
            self.logger.addHandler(handler)
            self.logger.info('Started run')
        else:
            self.logger = logging.getLogger('dummy')
            self.logger.addHandler(logging.NullHandler())
        #self.dbh = desdmdbi.DesDmDbi(config['des_services'], config['section'])
        self.autocommit(True)

    def dump_table(self, table):
        results = self.query_simple(table)
        print '\nTABLE',table
        for r in results:
            print '    ',r

    def delete_temp_files_from_db(self, delfiles):
        if delfiles:
            cur = self.cursor()
            cur.prepare('delete from %s where filename=:1' % (self.config['temp_table']))
            cur.executemany(None, delfiles)
            self.logger.info(' Successfully deleted %i rows' % cur.rowcount)
