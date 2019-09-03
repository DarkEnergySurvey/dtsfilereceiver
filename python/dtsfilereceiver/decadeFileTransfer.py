from dtsfilereceiver.globusConnection import GlobusConnection

import os
import dateutil.parser as dparser
from globus_sdk import TransferData
from dtsfilereceiver.decade_utils import to_night
import datetime
import pytz

def changed(task, item, val):
    """ Function to determine if a value has changed
    """
    if item in task:
        if task[item] != val:
            return True
    return False

class DecadeFileTransfer(GlobusConnection):
    """ Class for decade file transfers
    """
    def __init__(self, config):
        GlobusConnection.__init__(self, config, 'dft')
        self.nights = {}
        self.projects = []
        self.activity = False


    def update_transfers(self):
        """ Function to update the DB with current status of transfers
        """
        cur = self.cursor()
        cur.execute('SELECT propid FROM fast_transfer_propid UNION SELECT propid FROM propid_ignore where active=0')
        results = cur.fetchall()
        ig_pjt = []
        for res in results:
            ig_pjt.append(res[0])

        results = self.query_simple(self.config['data_table'], cols=['night', 'project', 'noao_path', 'globus_task_id', 'file_count', 'files_skipped', 'files_transferred'], where=['status=3'])
        for res in results:
            if res['project'] in ig_pjt:
                print "Ignoring %s -- fast transfer" % (res['project'])
                continue
            task = self.client.get_task(res['globus_task_id'])
            updatevals = {}
            if task.data['status'].upper() in ["ACTIVE", "SUCCEEDED"]:
                if changed(task.data, 'files', res['file_count']):
                    updatevals['file_count'] = int(task.data['files'])
                if changed(task.data, 'files_skipped', res['files_skipped']):
                    updatevals['files_skipped'] = int(task.data['files_skipped'])
                if changed(task.data, 'files_transferred', res['files_transferred']):
                    updatevals['files_transferred'] = int(task.data['files_transferred'])
                if task.data['status'] == "SUCCEEDED":
                    updatevals['status'] = 2
                    updatevals['bps'] = int(task.data['effective_bytes_per_second'])
                    self.number_successful += 1
                else:
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
                self.logger.info('Updated status of %s', res['globus_task_id'])
                self.basic_update_row(self.config['data_table'], updatevals, wherevals={'globus_task_id': res['globus_task_id']})
        self.logger.info('%i active transfers', self.active_transfer_count)
        self.logger.info('%i transfers marked as successful', self.number_successful)
        self.logger.info('%i transfers marked as failed', self.number_failed)
        self.activity = (self.number_successful + self.number_failed) > 0

    def get_new_nights(self):
        """ Function to get new entries from the Db

        """
        results = self.query_simple(self.config['data_table'], cols=['night', 'project'], where=['status=4'])
        for res in results:
            if res['night'] is None:
                self.projects.append(res['project'])
                continue
            if res['project'] is not None:
                res['project'] = res['project'].upper()
            self.nights[str(res['night'].date())] = (res['night'].date(), res['project'])
        self.logger.info('Found %i new entries to process', len(self.nights))
        self.logger.info('Found %i new full projects to process', len(self.projects))
        self.logger.info(' Scan complete')

    def things_to_do(self):
        """ Look to see if there is anything to do
        """
        if self.active_transfer_count >= self.TRANSLIMIT:
            self.logger.info('Max transfer count reached')
            self.activity = True
            return False
        if not self.nights and not self.projects:
            self.logger.info(' No new data')
            return False
        return True

    def exists(self, night, project, status_eq=None, status_ne=4):
        """ Check whether a night already exists in the DB
        """
        where = []
        if status_eq is not None:
            where.append('status=%i' % status_eq)
        if status_ne is not None:
            where.append('status!=%i' % status_ne)

        params = None
        if night is not None:
            where.append('trunc(night)=trunc(:q_night)')
            params = {'q_night': night}
        else:
            where.append('night is null')
        if project is not None:
            where.append('project=\'%s\'' % project)
        else:
            where.append('project is null')

        results = self.query_simple(self.config['data_table'], cols=['night', 'project'],
                                    where=where, params=params)

        return len(results) > 0


    def load_new_dirs(self):
        cur = self.cursor()
        cur.execute('select noao_path from noao_transfer where noao_path is not null')
        results = cur.fetchall()
        dirs = []
        process = []
        for res in results:
            dirs.append(res[0])
        for d in self.noao_dirs:
            if d not in dirs:
                process.append(d)
                print d
        print 'New dirs to check: ', len(process)

        if len(process) == 0:
            return
        cur.execute('SELECT propid FROM fast_transfer_propid UNION SELECT propid FROM propid_ignore where active=0')
        results = cur.fetchall()
        ig_pjt = []
        current_date = datetime.datetime.now()
        for res in results:
            ig_pjt.append(res[0])
        for ndir in process:
            loc = ndir.find('_')
            night = to_night(ndir[:loc])
            isCurrent = (current_date - datetime.datetime(night.year, night.month, night.day)).days < 2
            pjt = ndir[loc+1:]
            if pjt in ig_pjt or isCurrent:
                print pjt, pjt in ig_pjt, isCurrent
                continue
            self.basic_insert_row(self.config['data_table'], {'project': pjt, 'night': night, 'noao_path':ndir, 'detected':datetime.datetime.now()})


    def make_transfer(self, night, project, ndir):
        """ Function to create the data transfer object
        """
        self.active_transfer_count += 1
        tdata = TransferData(self.client,
                             self.config['src_ep'],
                             self.config['dest_ep'],
                             label='transfer %s_%s'%(night, project),
                             sync_level="checksum",
                             verify_checksum=True,
                             preserve_timestamp=True
                            )
        tdata.add_item(os.path.join(self.config['noao_root'], ndir),
                       os.path.join(self.config['transfer_dir'], ndir),
                       recursive=True)
        return tdata

    def initiate_transfer(self, transfers, dates, project):
        """ Initiate the transfer
        """
        self.activity = True
        trans_data, ndir, prjt = transfers[0]
        task = self.client.submit_transfer(trans_data)
        wherevals = {'night': dates[0]}
        if project is None:
            wherevals['project'] = prjt
        else:
            wherevals['project'] = project
        updatevals = {}

        if task.data['code'].lower() == 'accepted':
            updatevals = {'status': 3,
                          'noao_path': ndir,
                          'globus_task_id': str(task.data['task_id']),
                          'transfer_date': datetime.datetime.now()
                         }
        else:
            updatevals = {'status': 3,
                          'noao_path': ndir,
                          'globus_task_id': str(task.data['task_id']),
                          'notes': 'Possible error in status: %s' % str(task.data['code']),
                          'transfer_date': datetime.datetime.now()
                         }
        self.logger.info('Requested transfer of %s', (ndir))

        self.basic_update_row(self.config['data_table'], updatevals, wherevals)

        for i, trans in enumerate(transfers):
            if i == 0:
                continue
            trans_data, ndir, prjt = trans
            task = self.client.submit_transfer(trans_data)
            vals = {'night': dates[i],
                    'project': prjt,
                    'status':3,
                    'noao_path': ndir,
                    'globus_task_id': str(task.data['task_id']),
                    'transfer_date': datetime.datetime.now()
                   }

            if task.data['code'].lower() != 'accepted':
                vals['notes'] = 'Possible error in status: %s' % str(task.data['code'])

            self.logger.info('Requested transfer of %s', ndir)
            #try:
            #    self.basic_insert_row(self.config['data_table'], vals)
            #except:
            wherevals = {}
            for item in ['night', 'project']:
                wherevals[item] = vals[item]
            updatevals = {}
            for item in ['status', 'noao_path', 'globus_task_id', 'transfer_date']:
                updatevals[item] = vals[item]
            if 'notes' in vals:
                updatevals['notes'] = vals['notes']
            self.basic_update_row(self.config['data_table'], updatevals, wherevals)

    def process_nights(self):
        """ Process any available nights
        """
        self.logger.info('Processing %i night requests', len(self.nights))
        limit = False
        for night, (date, project) in self.nights.iteritems():
            if self.active_transfer_count >= self.TRANSLIMIT:
                if not limit:
                    self.logger.info('Transfer limit reached, stopping processing until queue shrinks.')
                limit = True
                self.number_waiting += 1
                continue
            initial = True
            transfers = []
            status = -2
            notes = 'Could not locate noao directory'

            for ndir in self.noao_dirs:
                if ndir.startswith(night.replace('-', '')):
                    if project is not None and project.upper() in ndir.upper():
                        if not self.exists(date, project):
                            transfers.append((self.make_transfer(night, project, ndir), ndir, project))
                            self.number_started += 1
                        else:
                            notes = 'Data already downloaded'
                            #status = -3
                        break
                    elif project is None:
                        loc = ndir.find('_')
                        pjt = ndir[loc+1:]
                        if not self.exists(date, pjt, status_ne=-5):
                            if initial:
                                self.basic_update_row(self.config['data_table'], {'project': pjt}, {'night': date, 'project': None})
                                initial = False
                            else:
                                self.basic_insert_row(self.config['data_table'], {'project': pjt, 'night': date, 'detected': datetime.datetime.now()})
                            transfers.append((self.make_transfer(night, ndir[loc+1:], ndir), ndir, ndir[loc+1:]))
                            self.number_started += 1
                        else:
                            notes = 'Data already downloaded'
                            #status = -3
            if not transfers:
                updatevals = {'notes': notes,
                              'status': status
                             }
                wherevals = {'night': date,
                             'project': project}
                self.basic_update_row(self.config['data_table'], updatevals, wherevals)
                self.logger.info(notes + ' for ' + night)
                continue
            self.logger.info('Initiating transfer of %i directories', len(transfers))
            self.initiate_transfer(transfers, [date] * len(transfers), project)

    def process_projects(self):
        """ Function to initiate the transfer of data for a project
        """
        self.logger.info('Processing %i projects', len(self.projects))
        limit = False
        for project in self.projects:
            initial = True
            transfers = []
            dates = []
            notes = 'Could not locate any matching projects'
            status = -2
            for ndir in self.noao_dirs:
                if project in ndir.upper():
                    night = to_night(ndir[:ndir.find('_')])
                    if not self.exists(night, project, None, None):
                        dates.append(night)
                        if self.active_transfer_count <= self.TRANSLIMIT:
                            transfers.append((self.make_transfer(str(night), project, ndir), ndir, project))
                            self.number_started += 1
                        else:
                            if not limit:
                                self.logger.info('Transfer limit reached, continuing to gather project data, but no additional transfers at this time.')
                                limit = True
                            self.number_waiting += 1

                        if initial:
                            updatevals = {'night': night}
                            wherevals = {'night' : None,
                                         'project': project}
                            self.basic_update_row(self.config['data_table'], updatevals, wherevals)
                            initial = False
                        else:
                            self.basic_insert_row(self.config['data_table'], {'project': project, 'night': night, 'detected': datetime.datetime.now()})
                    else:
                        notes = 'Data already downloaded'
                        #status = -3
            if not transfers and not limit:
                updatevals = {'notes': notes,
                              'status': status
                             }
                wherevals = {'night': None,
                             'project': project}
                self.basic_update_row(self.config['data_table'], updatevals, wherevals)
                self.logger.info(notes + ' for ' + project)
                continue
            if transfers:
                self.initiate_transfer(transfers, dates, project)

    def report(self):
        self.basic_insert_row('TRANSFER_MONITOR', {'run_date': self.starttime, 'number_started':self.number_started, 'number_active':self.active_transfer_count, 'number_successful':self.number_successful, 'number_failed':self.number_failed, 'number_waiting':self.number_waiting, 'scantime':self.scantime})
        if not self.activity:
            self.logger.info("No Activity")
