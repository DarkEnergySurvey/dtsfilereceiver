from dtsfilereceiver.decadeFileBase import DecadeFileBase

import os
import shutil
import glob
from fitsio import FITS
import datetime

class DecadeFilePrepare(DecadeFileBase):
    """ Class to prepare the files
    """
    def __init__(self, config, fast=False):
        print 'Fast', fast
        DecadeFileBase.__init__(self, config, 'fastP' if fast else 'dfp')
        #self.table_tag = 'fast_table' if fast else 'data_table'
        self.fast = fast
        self.starttime = datetime.datetime.now()
        self.number_scanned = 0
        self.number_processed = 0
        self.errors = 0
        self.activity = False

    def check_fast_complete(self):
        """ Check to see what files have been processed
        """
        #results = self.query_simple(self.config['fast_table'], cols='distinct night, project, globus_task_id')
        cur = self.cursor()
        gids = []
        for row in cur.execute("select distinct night, project, globus_task_id from %s where status=1" % (self.config['fast_table'])):
            gids.append(row)
        self.logger.info('Found %i directories to check', len(gids))
        # use direct sql calls here as there are no delete members of the base class

        #badfiles = glob.glob(os.path.join(self.config['bad_file_dir'], '*', '*', 'DECam*'))
        #print len(badfiles)
        for (nite, pid, gid) in gids:
            try:
                print nite, pid, gid
                results = self.query_simple(self.config['fast_table'], cols=['desfile_filename'], where="globus_task_id='%s'" % (gid))
                for result in results:
                    _file = result['desfile_filename']
                    if (len(self.query_simple('exposure', cols=['filename'], where="filename='%s'" % _file))) > 0:
                        self.basic_update_row(self.config['fast_table'], {'status': 0}, {'desfile_filename': _file, 'globus_task_id': gid})
                        self.activity = True
                cur.execute("select count(*) from %s where globus_task_id='%s' and status != 0" % (self.config['fast_table'], gid))
                count = cur.fetchone()[0]

                if count == 0:
                    self.activity = True
                    # since we have the cursor just keep using it
                    self.logger.info('%s %s ingestion is complete', str(nite), pid)
                    cur.execute("update %s set status=0 where globus_task_id=:q_gid" % (self.config['data_table']), {'q_gid':gid})
                    if cur.rowcount != 1:
                        raise Exception('Error updating row')
                else:
                    self.logger.info('Not complete yet: %i to go', count)
            except Exception as ex:
                self.logger.error('Error 1: %s', str(ex))


    def check_backgroud_complete(self):
        """ Check to see what files have been processed
        """
        results = self.query_simple(self.config['temp_table'], cols='distinct night, project, run_number')
        cur = self.cursor()
        self.logger.info('Found %i directories to check', len(results))
        # use direct sql calls here as there are no delete members of the base class

        badfiles = glob.glob(os.path.join(self.config['bad_file_dir'], '*', '*', 'DECam*'))
        #print len(badfiles)
        for result in results:
            try:
                cur.execute('select noao.filename from %s noao, exposure expo where trunc(noao.night)=trunc(:q_nt) and noao.project=:q_pj and noao.filename=expo.filename || \'.fz\' and run_number=:q_rid' % (self.config['temp_table']), {'q_nt':result['night'], 'q_pj':result['project'], 'q_rid':result['run_number']})
                res = cur.fetchall()
                cur.execute('select noao.filename from %s noao where trunc(noao.night)=trunc(:q_nt) and noao.project=:q_pj and run_number=:q_rid' % (self.config['temp_table']), {'q_nt':result['night'], 'q_pj':result['project'], 'q_rid':result['run_number']})
                allfiles = cur.fetchall()
                delfiles = []

                for name in allfiles:
                    #print name[0]
                    for bf in badfiles:
                        if name[0] in bf:
                            delfiles.append(name)
                print result['night'], result['project'], len(res), len(delfiles)
                for name in res:
                    delfiles.append((name[0],))
                if delfiles:
                    self.activity = True
                    self.logger.info('Deleting %i entries for %s  %s', len(delfiles), str(result['night']), result['project'])
                    self.delete_temp_files_from_db(delfiles)
                    # since we have the cursor just keep using it
                    cur.execute('select count(*) from %s where night=:q_nt and project=:q_pj and run_number=:q_rid' % (self.config['temp_table']), {'q_nt':result['night'], 'q_pj':result['project'], 'q_rid':result['run_number']})
                    rres = cur.fetchall()
                    if rres[0][0] == 0:
                        self.logger.info('%s %s ingestion is complete', str(result['night']), result['project'])
                        cur.execute('update %s set status=0 where night=:q_nt and project=:q_pj and run_number=:q_rid' % (self.config['data_table']), {'q_nt':result['night'], 'q_pj':result['project'], 'q_rid':result['run_number']})
                        if cur.rowcount != 1:
                            raise Exception('Error updating row')
                    else:
                        self.logger.info('%s %s Not complete yet', str(result['night']), result['project'])
            except Exception as ex:
                self.logger.error('Error 1: %s', str(ex))
        try:
            cur.execute('select night, project, run_number from noao_transfer where status=1 and fast=0')
            results = cur.fetchall()
            uplist = []
            cur.prepare("select count(*) from noao_temp where night=:q_nt and project=:q_prj and run_number=:q_rid")
            for res in results:
                cur.execute(None, {'q_nt': res[0], 'q_prj': res[1], 'q_rid': res[2]})
                res2 = cur.fetchall()
                count = res2[0][0]
                if count == 0:
                    uplist.append({'q_nt': res[0], 'q_prj': res[1], 'q_rid': res[2]})
            if uplist:
                self.activity = True
                cur.executemany("update noao_transfer set status=0 where night=:q_nt and project=:q_prj and run_number=:q_rid", uplist)
            for i in uplist:
                self.logger.info('Marking %s  %s, run %i as complete (no more files to be ingested)' % (i['q_nt'], i['q_prj'], i['q_rid']))

        except Exception as ex:
            self.logger.error('Error 2: %s', str(ex))

    def check_for_complete(self):
        if self.fast:
            self.check_fast_complete()
        else:
            self.check_backgroud_complete()

    def process_file(self, result, fname, root):
        #print 'RENAME',root,fname
        os.rename(root, fname)
        #print 'MOVE',fname, os.path.join(self.config['delivery_dir'], fname)
        shutil.move(fname, os.path.join(self.config['delivery_dir'], fname))
        with open(os.path.join(self.config['delivery_notice_dir'], fname + '.dts'), 'w') as handle:
            handle.write('name=%s\n' % fname)
        return {'night':result['night'],
                'project':result['project'],
                'filename':fname}


    def get_filename_from_fits(self, ffile, ftype):
        fname = None
        try:
            f = FITS(ffile)
            h = f[0].read_header()
            expnum = h['EXPNUM']
            fname = 'DECam_%08i' % expnum
            fname += '.' + ftype
            f.close()
        except Exception as e:
            self.logger.info('ERROR ' + str(e))
        finally:
            return fname

    def get_filename_from_hdr_file(self, ffile):
        fname = None
        root = None
        try:
            root = ffile.replace('.hdr', '.fits.fz')
            with open(ffile) as handle:
                for line in handle.readlines():
                    if 'FILENAME' in line:
                        fname = line.split()[1]
                        fname = fname.replace("'", '')
                        fname += '.fz'
                        break
        except Exception as ex:
            self.logger.info('ERROR ' + str(ex))
        finally:
            return root, fname

    def process_fast_dirs(self, result):
        # night,project,noao_path,globus_task_id
        start = datetime.datetime.now()
        full_list = self.query_simple(self.config['fast_table'], cols=['filename'], where=["globus_task_id='%s' and filename like '%%.fits%%'" % result['globus_task_id']])
        files = set()
        for _file in full_list:
            files.add(_file['filename'])
        curs = self.cursor()
        fails = 0
        proc = 0
        for _file in files:
            sloc = _file.find('.')
            suf = _file[sloc + 1:]
            hdrf = _file.replace('.' + suf, '.hdr')
            fname = self.get_filename_from_fits(_file, suf)
            if fname is not None:
                _ = self.process_file(result, fname, _file)
                proc += 1
                try:
                    os.remove(hdrf)
                except:
                    pass
            else:
                try:
                    root, fname = self.get_filename_from_hdr_file(hdrf)
                    if root is not None:
                        try:
                            _ = self.process_file(result, fname, root)
                            proc += 1
                        except:
                            fails += 1
                        os.remove(hdrf)
                except:
                    fails += 1
            curs.execute("delete from %s where filename='%s' and globus_task_id='%s'" % (self.config['fast_table'], hdrf, result['globus_task_id']))
            curs.execute('commit')
            self.basic_update_row(self.config['fast_table'], {'status': 1, 'desfile_filename': fname.replace('.fz', '')}, {'filename': _file, 'globus_task_id': result['globus_task_id']})

        updatevals = {'status': 1, 'prepare_start': start, 'prepare_end': datetime.datetime.now()}
        wherevals = {'globus_task_id': result['globus_task_id']}

        self.basic_update_row(self.config['data_table'], updatevals, wherevals)

        self.number_processed += proc
        self.errors += fails
        self.logger.info('Processed %s %s: %i files    %i fails', result['night'], result['project'], proc, fails)
        if proc != 0 or fails != 0:
            self.activity = True

        #for i in range(len(updates)):
        #    self.basic_update_row(self.config[self.table_tag], updates[i], wherevals[i])

        os.chdir(self.config['transfer_dir'])

    def process_background_dirs(self, result):
        """ Process any available directories
        """
        start = datetime.datetime.now()
        inserts = []
        fc = False
        for suf in ['fits.fz', 'fits']:
            files = glob.glob('*.' + suf)
            for _file in files:
                fname = self.get_filename_from_fits(_file, suf)
                if fname is not None:
                    inserts.append(self.process_file(result, fname, _file))
                    try:
                        hdrf = _file.replace('.' + suf, '.hdr')
                        os.remove(hdrf)
                    except:
                        pass
                else:
                    fc = True
        if fc:
            print "M2"
            files = glob.glob('*.hdr')
            for _file in files:
                root, fname = self.get_filename_from_hdr_file(_file)
                if root is not None:
                    try:
                        inserts.append(self.process_file(result, fname, root))
                        os.remove(_file)
                    except:
                        pass
        fails = len(glob.glob('*.fz')) + len(glob.glob('*.fits'))
        self.number_processed += len(inserts)
        self.errors += fails

        self.logger.info('Processed %s %s: %i files    %i fails', result['night'], result['project'], len(inserts), fails)
        if len(inserts) != 0 or fails != 0:
            self.activity = True
        #if len(fails) == 0:
        #    for f in glob.glob("*.hdr"):
        #        os.remove(f)
        updatevals = {'status': 1, 'prepare_start': start, 'prepare_end': datetime.datetime.now()}
        wherevals = {'globus_task_id': result['globus_task_id']}

        self.basic_update_row(self.config['data_table'], updatevals, wherevals)
        try:
            self.insert_many(self.config['temp_table'], columns=['night', 'project', 'filename'], rows=inserts)
        except:
            pass

        os.chdir(self.config['transfer_dir'])
        try:
            os.rmdir(os.path.join(self.config['transfer_dir'], result['noao_path']))
        except OSError:
            self.logger.warning('Could not remove %s, it is not empty.', os.path.join(self.config['transfer_dir'], result['noao_path']))

    def process_dirs(self):
        count = 0
        try:
            cur = self.cursor()
            cur.execute('SELECT propid FROM fast_transfer_propid')
            results = cur.fetchall()
            ft_pjt = []
            for res in results:
                ft_pjt.append(res[0])

            results = self.query_simple(self.config['data_table'], cols='distinct night,project,noao_path,globus_task_id,fast', where='status=2')
            self.logger.info('Fount %i new directories to process', len(results))
            self.number_scanned += len(results)
            for result in results:
                self.logger.info(' Processing ' + str(result))
                if not self.fast and (result['project'] in ft_pjt or result['fast'] == 1):
                    self.logger.info('    Skipping - fast transfer')
                    continue
                if self.fast and result['project'] not in ft_pjt and result['fast'] == 0:
                    self.logger.info('    Skipping - normal project')
                    continue
                count += 1
                try:
                    os.chdir(os.path.join(self.config['transfer_dir'], result['noao_path']))
                    if self.fast:
                        self.process_fast_dirs(result)
                    else:
                        self.process_background_dirs(result)
                except Exception as ex:
                    self.logger.error('Error: %s', str(ex))
        except Exception as ex:
            self.logger.error('Error 3: %s', str(ex))
        finally:
            self.logger.info('Processed %i directories' % count)


    def report(self):
        self.basic_insert_row('PREPARE_MONITOR', {'run_date': self.starttime, 'directories_scanned':self.number_scanned, 'files_processed':self.number_processed, 'errors':self.errors})
        if not self.activity:
            self.logger.info("No Activity")
