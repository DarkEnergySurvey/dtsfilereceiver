#!/usr/bin/env python
""" This is a unit test suite for the decade file transfer code

"""
import unittest
import os
import binascii
import dtsfilereceiver.decade_utils as decutils
import despydmdb.desdmdbi as desdmdbi
import datetime


class DummyTask(object):
    """ Class to mimic a Globus Task
    """
    def __init__(self, **kwargs):
        self.data = {'id': binascii.b2a_hex(os.urandom(15)),
                     'task_id': binascii.b2a_hex(os.urandom(15)),
                     'status': None,
                     'files': 0,
                     'files_skipped': 0,
                     'files_transferred': 0,
                     'effective_bytes_per_second': 0,
                     'description': None,
                     'completion_time': None,
                     'fatal_error': {},
                     'code': None}
        for k, val in kwargs.iteritems():
            self.data[k] = val

class DummyClient(object):
    """ Class to mimic a Globus Client
    """
    def __init__(self, tasks=None):
        self.tasks = {}
        if not tasks:
            return
        for task in tasks:
            self.tasks[task.data['id']] = task

    def add_task(self, task):
        """ Add a task
        """
        self.tasks[task.data['id']] = task

    def get_task(self, task_id):
        """ Get a task by ID
        """
        return self.tasks[task_id]

    def submit_transfer(self, task_data):
        """ Fake a transfer job submission
        """
        if 'label' in task_data.data and 'fail' in task_data.data['label'].lower():
            return DummyTask(code='Error')
        return DummyTask(code='Accepted')

class DummyTransfer(object):
    """ Class to mimic a Globus Transfer Task
    """
    def __init__(self, *args, **kwargs):
        self.data = {}
        self.args = args
        for k, v in kwargs.iteritems():
            self.data[k] = v

    def add_item(self, *args, **kwargs):
        """ Just so the code does not throw an error
        """
        pass

class TestDTF(unittest.TestCase):
    """ Class to test the DecadeFileTransfer class
    """
    @classmethod
    def setUpClass(cls):
        """ Stuff the needs to be initialized for all of the tests, but only done once
        """
        # override the import so that we can use the dummy classes and not have to worry
        #   about globus connections
        decutils.TransferData = DummyTransfer

        # initial nights
        cls.good_nights = [{'night': datetime.datetime(2000,1,1), 'project': None},
                           {'night': datetime.datetime(2000,1,2), 'project': '2000B-0002'}
                          ]

        # initial project
        cls.good_projects = [{'night': None, 'project': '2000A-0001'}]

        # initialize the DFT class
        cls.dft = decutils.DecadeFileTransfer(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'decade_test.cfg'))
        cls.tables = ['temp_table', 'data_table']

        # directory listing to mimic that of the NOAO archive
        cls.dft.noao_dirs = ['20000101_2000A-0005',
                             '20000101_2001B-0060',
                             '20000102_2000B-0002',
                             '20000102_2006A-0100',
                             '20000103_2001C-0030',
                             '20000103_2002A-0009',
                             '20000104_2000D-0008',
                             '20000104_2003A-0010',
                             '20000303_2000A-0001',
                             '20000402_2000A-0001',
                             '20000202_2000A-0001',
                             '20000707_fail'
                            ]
        # set up a DB connection
        cls.dbh = desdmdbi.DesDmDbi(cls.dft.config['des_services'], cls.dft.config['des_db_section'])
        cls.dbh.autocommit(True)
        curs = cls.dbh.cursor()
        # create the test tables
        sql = '''
            CREATE TABLE %s (
                "NIGHT" DATE DEFAULT NULL,
                "PROJECT" VARCHAR2(20 BYTE) DEFAULT null,
                "TRANSFER_DATE" TIMESTAMP (6) DEFAULT null,
                "STATUS" NUMBER(2,0) DEFAULT 4,
                "GLOBUS_TASK_ID" VARCHAR2(50 BYTE) DEFAULT null,
                "FILE_COUNT" NUMBER(8,0) DEFAULT null,
                "FILES_SKIPPED" NUMBER(8,0) DEFAULT 0 NOT NULL ENABLE,
                "FILES_TRANSFERRED" NUMBER(8,0) DEFAULT 0 NOT NULL ENABLE,
                "BPS" NUMBER(15,0) DEFAULT null,
                "NOAO_PATH" VARCHAR2(40 BYTE) DEFAULT null,
                "NOTES" VARCHAR2(200 BYTE) DEFAULT null,
                CONSTRAINT "TTEST_UNIQUE" UNIQUE ("NIGHT", "PROJECT"),
                CONSTRAINT "TEST_VALID" CHECK ("NIGHT" is not null or "PROJECT" is not null)
            )

        ''' % (cls.dft.config['data_table'])
        curs.execute(sql)
        sql = '''
            CREATE TABLE %s (
                "NIGHT" DATE NOT NULL,
                "PROJECT" VARCHAR2(20 BYTE) NOT NULL,
                "FILENAME" VARCHAR2(40) NOT NULL,
                "NOTES" VARCHAR2(200 BYTE) DEFAULT null,
                constraint TEST_UNQ UNIQUE("FILENAME"),
                constraint TEST_XFER_FK Foreign Key ("NIGHT","PROJECT") references %s("NIGHT","PROJECT")
            )
        ''' % (cls.dft.config['temp_table'], cls.dft.config['data_table'])
        curs.execute(sql)

    @classmethod
    def tearDownClass(cls):
        """ Clean up at the end
        """
        # drop any test tables that exist
        if cls.dbh is not None:
            curs = cls.dbh.cursor()
            for tbl in cls.tables:
                try:
                    curs.execute('drop table %s purge' % cls.dft.config[tbl])
                except Exception as ex:
                    print 'err',str(ex)

            cls.dbh = None
    
    def __del__(cls):
        """ Overload the del method so test tables get cleaned up
        """
        if cls.dbh is not None:
            curs = cls.dbh.cursor()
            for tbl in cls.tables:
                try:
                    print 'drop table %s purge\n' % cls.dft.config[tbl]
                    curs.execute('drop table %s purge' % cls.dft.config[tbl])
                except Exception as ex:
                    print 'err',str(ex)
            cls.dbh = None
                    

    def ingest(self, data):
        """ Helper function for an insert
        """
        cur = self.dbh.cursor()
        cur.prepare('insert into %s (NIGHT, PROJECT) values (:night, :project)' % (self.dft.config['data_table']))
        cur.executemany(None, data)

    def setUp(self):
        """ Stuff that needs to be set up for each test
        """
        # ingest initial data
        self.ingest(self.good_nights + self.good_projects)
        # set up dummy client
        self.dft.client = DummyClient()

    def tearDown(self):
        """ Clean up after each test
        """
        # delete any data from the test tables
        cur = self.dbh.cursor()
        for tbl in self.tables:
            cur.execute('delete from %s' % self.dft.config[tbl])

    def get_in_progress(self, status):
        """ helper function to get 'in progress' transfers
        """
        return self.dbh.query_simple(self.dft.config['data_table'],
                                     cols=['night',
                                           'noao_path',
                                           'globus_task_id',
                                           'file_count',
                                           'files_skipped',
                                           'files_transferred'], 
                                     where=['status=%i' % status])

    def create_row(self, task, night, project, path, status=3):
        """ helper function to insert a row
        """
        vals = {'night': night,
                'project': project,
                'noao_path': path,
                'globus_task_id': task.data['id'],
                'status': status}
        self.dbh.basic_insert_row(self.dft.config['data_table'], vals)

    def dump_table(self, table):
        """ method to print out a table
        """
        results = self.dbh.query_simple(table)
        print '\n'
        print '------------------------------------------------'
        for r in results:
            print r
        print '\n\n'

    def test_update(self):
        """ Test for update_transfers function
        """
        # set up dummy tasks
        task_complete = DummyTask(status='SUCCEEDED',
                                  file_count=100,
                                  files_skipped=1,
                                  files_transferred=99,
                                  effective_bytes_per_second=123456,
                                  completion_time='2000-01-02 15:22:15')
        task_in_progress = DummyTask(status='ACTIVE',
                                     file_count=1500,
                                     files_transferred=150)
        task_failed = DummyTask(status='FAILED',
                                fatal_error={'code':999},
                                description='The task failed.')
        self.dft.client = DummyClient([task_complete, task_in_progress, task_failed])
        self.create_row(task_complete, datetime.date(2000,1,1), 'ABCD', '/tmp')
        self.create_row(task_in_progress, datetime.date(2000,1,2), 'BDEFG', '/tmp2')
        self.create_row(task_failed, datetime.date(2000,1,3), 'QWERT', '/tmp3')
        self.dft.update_transfers()
        self.assertEqual(len(self.get_in_progress(3)), 1)
        self.assertEqual(len(self.get_in_progress(2)), 1)
        self.assertEqual(len(self.get_in_progress(-1)), 1)

    def test_get_new_nights(self):
        """ Test for get_new_nights function
        """
        self.dft.get_new_nights()
        self.assertEqual(len(self.dft.nights), len(self.good_nights))
        self.assertEqual(len(self.dft.projects), len(self.good_projects))

    def test_exists(self):
        """ Tests for exists function
        """
        self.dbh.basic_update_row(self.dft.config['data_table'],{'status':3},{'status':4})
        self.assertTrue(self.dft.exists(datetime.date(2000,1,1), None))
        self.assertTrue(self.dft.exists(None, '2000A-0001'))
        self.assertTrue(self.dft.exists(datetime.date(2000,1,2), '2000B-0002'))
        self.assertFalse(self.dft.exists(datetime.date(2000,1,2), None))
        self.assertFalse(self.dft.exists(datetime.date(2000,12,12), 'TEST'))

    def test_process_night(self):
        """ Test for process_night function
        """
        already_dl = [{'night': datetime.date(2000,1,2), 'project': None},
                      {'night': datetime.date(2002,4,4), 'project': None}]
        self.dft.nights = {'20000101': (datetime.date(2000,1,1), None),
                            '20000102': (datetime.date(2000,1,2), '2000B-0002')}
        self.dft.process_nights()
        self.assertEqual(len(self.get_in_progress(3)), 3)
        self.ingest(already_dl)
        self.dft.nights = {'20000102': (datetime.date(2000,1,2), None),
                           '20020404': (datetime.date(2002,4,4), None)}

        self.dft.process_nights()
        self.assertEqual(len(self.get_in_progress(3)), 4)
        self.assertEqual(len(self.get_in_progress(-2)), 1)


    def test_process_projects(self):
        """ Test for process_projects function
        """
        new_prj = [{'night': datetime.date(2000,2,2), 'project': '2000A-0001'},
                   {'night': None, 'project': '1999A-0000'}]
        self.ingest(new_prj)
        self.dft.projects = ['2000A-0001', '1999A-0000']
        self.dft.process_projects()
        self.assertEqual(len(self.get_in_progress(3)), 2)
        self.assertEqual(len(self.get_in_progress(-2)), 1)

    def test_globus_submit_fail(self):
        """ Test in the case of a submit failure
        """
        self.ingest([{'night': datetime.date(2000,7,7), 'project': 'fail'}])
        self.dft.nights = {'20000707': (datetime.date(2000,7,7), 'fail')}
        self.dft.process_nights()
        self.assertEqual(len(self.get_in_progress(3)), 1)

if __name__ == '__main__':
    unittest.main(verbosity=2)
