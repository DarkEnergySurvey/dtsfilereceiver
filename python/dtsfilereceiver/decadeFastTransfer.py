from dtsfilereceiver.globusConnection import GlobusConnection

import os
import datetime
import pytz
import dateutil.parser as dparser
from globus_sdk import TransferData
from dtsfilereceiver.decade_utils import to_night

class DecadeFastTransfer(GlobusConnection):
    def __init__(self, config):
        GlobusConnection.__init__(self, config, 'fastX')
        self.indiv_files = {}
        self.activity = False

    def scan_dir(self, dirname, night, pjt):
        full_dirs = self.client.operation_ls(self.config['src_ep'], path=self.config['noao_root'] + '/' + dirname)
        results = self.query_simple(self.config['fast_table'], cols=['filename', 'globus_task_id'], where="noao_path='%s'" % (dirname))
        existingFiles = []
        gid = []
        for res in results:
            existingFiles.append(res['filename'])
            gid.append(res['globus_task_id'])
        if dirname not in self.indiv_files.keys():
            self.indiv_files[dirname] = {}
            tempfls = []
        count = 0
        #print full_dirs
        curs = self.cursor()
        curs.execute("select count(*) from %s where project='%s' and trunc(night)=trunc(:q_ngt)" % (self.config['data_table'], pjt), {':q_ngt':night})
        runnum = curs.fetchone()[0]

        for entry in full_dirs:
            if entry['type'].lower() == 'file':
                #print str(entry['name'])
                if str(entry['name']) in existingFiles:
                    #print gid[existingFiles.index(str(entry['name']))]
                    #print "    CONTIN"
                    continue
                try:
                    self.basic_insert_row(self.config['fast_table'], {'project': pjt, 'night': night, 'noao_path':dirname, 'filename':str(entry['name']), 'run_number': runnum})
                    tempfls.append(str(entry['name']))
                    #print pjt, night, dirname, str(entry['name'])
                    count += 1
                except:
                    #print "    EXCET"
                    pass
        if count > 0:
            #for i in range(50):
            #    try:
            self.basic_insert_row(self.config['data_table'], {'project': pjt, 'night': night, 'noao_path':dirname, 'run_number':runnum, 'detected':datetime.datetime.now(), 'fast': 1})
            if runnum not in self.indiv_files[dirname].keys():
                self.indiv_files[dirname][runnum] = tempfls
            else:
                self.indiv_files[dirname][runnum] += tempfls
            #        break
            #    except:
            #        if i == 49:
            #            raise
            #        pass
                #self.basic_update_row(self.config['data_table'], {'status': 4}, {'project': pjt, 'night': night,'noao_path':dirname})
        else:
            dt = datetime.datetime.now() - datetime.datetime(night.year, night.month, night.day)
            if dt.days >= 2:
                self.basic_update_row(self.config['data_table'], {'status': 0}, {'project': pjt, 'night': night, 'noao_path':dirname})
                self.logger.info('More than 2 days since %s, marking %s as complete.', str(night), dirname)
            del self.indiv_files[dirname]


    def update_transfers(self):
        """ Function to update the DB with current status of transfers
        """
        results = self.query_simple(self.config['fast_table'], cols=['night', 'filename', 'noao_path', 'globus_task_id'], where=['status=3'])
        gtid = set()
        for res in results:
            gtid.add(res['globus_task_id'])
        for gid in gtid:
            print gid
            task = self.client.get_task(gid)
            updatevals = {}
            if task.data['status'].upper() in ["ACTIVE", "SUCCEEDED"]:
                updatevals['file_count'] = int(task.data['files'])
                updatevals['files_skipped'] = int(task.data['files_skipped'])
                updatevals['files_transferred'] = int(task.data['files_transferred'])
                if task.data['status'] == "SUCCEEDED":
                    print "SUCCESS"
                    updatevals['status'] = 2
                    updatevals['bps'] = int(task.data['effective_bytes_per_second'])
                    self.number_successful += 1
                else:
                    print "ACTIVE"
                    self.active_transfer_count += 1
            elif task.data['status'] == "FAILED":
                self.number_failed += 1
                updatevals['status'] = -1
                failed = str(task.data['fatal_error']['code']) + ': ' + str(task.data['fatal_error']['description'])
                if len(failed) > 199:
                    failed = failed[:198] + '?'
                updatevals['notes'] = failed
            if task.data['completion_time']:
                temptime = dparser.parse(str(task.data['completion_time']))
                temptime.replace(tzinfo=pytz.UTC)
                updatevals['transfer_end'] = temptime.astimezone(pytz.timezone('US/Central'))
            if updatevals:
                print updatevals
                self.logger.info('Updated status of %s', gid)
                self.basic_update_row(self.config['data_table'], updatevals, wherevals={'globus_task_id':gid})
                if 'transfer_end' in updatevals.keys():
                    updatevals['transfer_date'] = updatevals['transfer_end']
                    del updatevals['transfer_end']
                for item in ['file_count', 'files_skipped', 'files_transferred']:
                    if item in updatevals.keys():
                        del updatevals[item]
                print updatevals
                if len(updatevals) > 0:
                    self.basic_update_row(self.config['fast_table'], updatevals, wherevals={'globus_task_id':gid})
        self.logger.info('%i active transfers', self.active_transfer_count)
        self.logger.info('%i transfers marked as successful', self.number_successful)
        self.logger.info('%i transfers marked as failed', self.number_failed)
        self.activity = (self.number_successful + self.number_failed) > 0

    def get_new_files(self):
        """ Function to get new entries from the Db

        """
        results = self.query_simple(self.config['fast_table'], cols=['noao_path', 'filename', 'run_number'], where=['status=4'])
        count = 0
        for res in results:
            if res['noao_path'] not in self.indiv_files.keys():
                self.indiv_files[res['noao_path']] = {}
            if res['run_number'] not in self.indiv_files[res['noao_path']].keys():
                self.indiv_files[res['noao_path']][res['run_number']] = []
            self.indiv_files[res['noao_path']][res['run_number']].append(res['filename'])
            count += 1
        self.logger.info('Found %i new dirs to process with %i files', len(self.indiv_files), count)
        self.logger.info(' Scan complete')

    def things_to_do(self):
        """ Look to see if there is anything to do
        """
        if self.active_transfer_count >= self.TRANSLIMIT:
            self.logger.info('Max transfer count reached')
            self.activity = True
            return False
        if not self.indiv_files:
            self.logger.info(' No new data')
            return False
        return True

    def load_new_files(self):
        cur = self.cursor()
        cur.execute('select noao_path, status from noao_transfer where noao_path is not null')
        results = cur.fetchall()
        dirs = []
        process = []
        stat = []
        for res in results:
            if res[1] == 2:
                print res[1]
            dirs.append(res[0])
            stat.append(res[1])
        for d in self.noao_dirs:
            if d not in dirs:
                process.append(d)
            elif stat[dirs.index(d)] == 2:
                process.append(d)

        print 'New dirs to process: ', len(process)

        if len(process) == 0:
            return
        cur.execute('SELECT propid FROM fast_transfer_propid')
        results = cur.fetchall()
        ft_pjt = []
        for res in results:
            ft_pjt.append(res[0])
        #print ft_pjt
        current_date = datetime.datetime.now()
        for ndir in process:
            loc = ndir.find('_')
            night = to_night(ndir[:loc])
            pjt = ndir[loc+1:]
            isCurrent = (current_date - datetime.datetime(night.year, night.month, night.day)).days < 2
            #print ndir
            if pjt in ft_pjt or isCurrent:
                #if pjt in ['2019A-0305', '2019A-0065', '2019A-0205', '2019A-0240', '2018A-0177', '2019A-0235']:
                #if pjt in ['2019A-0235']:
                print ndir, isCurrent, (current_date - datetime.datetime(night.year, night.month, night.day)).days
                self.scan_dir(ndir, night, pjt)
                #break

    def make_transfer(self, path):
        """ Function to create the data transfer object
        """
        self.active_transfer_count += 1
        tdata = TransferData(self.client,
                             self.config['src_ep'],
                             self.config['dest_ep'],
                             label='transfer %s'%(path),
                             sync_level="checksum",
                             verify_checksum=True,
                             preserve_timestamp=True
                            )
        return tdata

    def process_files(self):
        """ Process any available nights
        """
        self.logger.info('Processing %i file requests', len(self.indiv_files))
        limit = False
        for path, run in self.indiv_files.iteritems():
            #print '\n',run,'\n'
            for runid, filenames in run.iteritems():
                if len(filenames) == 0:
                    continue
                self.activity = True
                if self.active_transfer_count >= self.TRANSLIMIT:
                    if not limit:
                        self.logger.info('Transfer limit reached, stopping processing until queue shrinks.')
                        limit = True
                    self.number_waiting += 1
                    continue
                #initial = True
                transfer = None
                #status = -2
                #notes = 'Could not locate noao directory'

                transfer = self.make_transfer(path)
                for filename in filenames:
                    transfer.add_item(os.path.join(self.config['noao_root'], path, filename),
                                      os.path.join(self.config['transfer_dir'], path, filename))

                self.logger.info('Initiating transfer of %s directories', path)
                self.initiate_transfer(transfer, path, filenames, runid)
                self.number_started += 1

    def initiate_transfer(self, transfer, path, filenames, runid):
        """ Initiate the transfer
        """
        task = self.client.submit_transfer(transfer)
        wherevals = {'noao_path': path, 'run_number': runid}
        updatevals = {}

        if task.data['code'].lower() == 'accepted':
            updatevals = {'status': 3,
                          'globus_task_id': str(task.data['task_id']),
                          'transfer_date': datetime.datetime.now()
                         }
        else:
            updatevals = {'status': 3,
                          'globus_task_id': str(task.data['task_id']),
                          'notes': 'Possible error in status: %s' % str(task.data['code']),
                          'transfer_date': datetime.datetime.now()
                         }
        self.logger.info('Requested transfer of %s', path)

        #for filename in filenames:
        #wherevals['filename'] = filename
        self.basic_update_row(self.config['fast_table'], updatevals, wherevals)
        #del wherevals['filename']
        #wherevals['run_number'] = runid
        self.basic_update_row(self.config['data_table'], updatevals, wherevals)

    def report(self):
        self.basic_insert_row('TRANSFER_MONITOR', {'run_date': self.starttime, 'number_started':self.number_started, 'number_active':self.active_transfer_count, 'number_successful':self.number_successful, 'number_failed':self.number_failed, 'number_waiting':self.number_waiting, 'scantime':self.scantime})
        if not self.activity:
            self.logger.info("No Activity")
