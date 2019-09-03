#!/usr/bin/env python
# $Id: dts_monitor.py 48726 2019-08-06 14:59:19Z friedel $
# $Rev:: 48726                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-08-06 09:59:19 #$:  # Date of last commit.

""" Create a web page used to monitor the DESDM side of DTS """

####### Design comment:   Never extra if delivered=False, but missing if delivered=True

import argparse
from datetime import datetime, timedelta
import sys
import os
import glob
import re
import pytz
import getpass
import socket
import urllib2
import subprocess
from dateutil.parser import parse

import despydb.desdbi as desdbi
import despymisc.create_special_metadata as smfuncs


verbose = False
DEFAULT_NUM_NITES = 48
NITE_COLORS = ['silver', 'olive', 'grey', 'teal']
#NITESUM_BASE = "http://decam03.fnal.gov:8080/nightsum/"
NITESUM_BASE = "http://des-ops.fnal.gov:8080/nightsum/"
dts_archive_path = '/archive_data/desarchive/DTS'

# desar2
#DELIVERY_LOG = "/home/databot/dts.log"
#ACCEPT_LOG_DIR = "/home/databot2/logs/accept_dts_delivery_logs"
#HANDLER_LOG_DIR = "/home/databot2/logs/dts_file_handler_logs"

# desar3
#DELIVERY_LOG = "/local/dts/logs/dts.log"
#DELIVERY_DIR = "/local/dts/delivery"
#ACCEPT_LOG_DIR = "/local/dts_desdm/logs/accept_dts_delivery_logs"
#HANDLER_LOG_DIR = "/local/dts_desdm/logs/dts_file_handler_logs"
#FAIL_DIR = "/des006/DTSfail"

# decade
DELIVERY_DIR = "/decade/incoming/dts/delivery"
HANDLER_LOG_DIR = "/decade/incoming/dts/logs/ingest"
FAIL_DIR = "/decade/incoming/dts/DTSfail"

desdm_db_failure = False
sispi_db_failure = False



######################################################################
def combine_sne_info(nitelist, info):

    sispi_sne = info['sispi_sne']
    desdm_sne = info['desdm']
    manexposures = info['manifest_exposures']
    manifests = info['manifests']

    html_info = {}
    for nite in nitelist:
        html_info[nite] = {}
        for expnum in sispi_sne[nite]:
            html_info[nite][expnum] = sispi_sne[nite][expnum]
            hdict = html_info[nite][expnum]
            if expnum in desdm_sne:
                hdict['desdm'] = 'Y'
                hdict['enite'] = desdm_sne[expnum]['nite']
                hdict['efield'] = desdm_sne[expnum]['field']
                hdict['eband'] = desdm_sne[expnum]['band']
                hdict['eexptime'] = desdm_sne[expnum]['exptime']
            else:
                hdict['desdm'] = 'N'
                hdict['enite'] = None
                hdict['efield'] = None
                hdict['eband'] = None
                hdict['eexptime'] = None

            if nite in manifests and hdict['field'] in manifests[nite] and \
                    hdict['band'] in manifests[nite][hdict['field']]:
                hdict['desdm_manifest'] = 'Y'
                #print nite, hdict['field'], hdict['band'], manifests[nite][hdict['field']][hdict['band']]
                hdict['manifest'] = ','.join(manifests[nite][hdict['field']][hdict['band']])
            else:
                #print "Missing manifest for:   nite: %s, field: %s, band: %s" % (nite, hdict['field'], hdict['band'])
                hdict['desdm_manifest'] = 'N'


            if expnum in manexposures:
                mdict = manexposures[expnum]
                hdict['meexptime'] = mdict['exptime']
                hdict['mefield'] = mdict['field']
                hdict['meband'] = mdict['band']
                hdict['manifest'] = mdict['manifest_filename']
            else:
                hdict['meexptime'] = None
                hdict['mefield'] = None
                hdict['meband'] = None

    return html_info


######################################################################
def count_num_files_in_delivery_dir():
    """ Count the number of files in the delivery directory """

    cnt = None
    if os.path.exists(DELIVERY_DIR):
        cnt = len(os.listdir(DELIVERY_DIR))
    return cnt


######################################################################
def convert_dts_log_timestamp(dts_ts):
    """ convert dts log timestamp to local timestamp """
    local_dt = None

    tsmatch = re.match(r'(\d\d)(\d\d) (\d\d:\d\d:\d\d)', dts_ts)
    if tsmatch:
        month = int(tsmatch.group(1))
        day = tsmatch.group(2)
        time = tsmatch.group(3)

        curr_time = datetime.now()
        if month > curr_time.month:
            year = curr_time.year -1
        else:
            year = curr_time.year
        new_ts_str = "%s-%02d-%sT%s.000000+00:00" % (year, month, day, time)
        print "MMG new_ts_str = %s" % new_ts_str
        utc_dt = parse(new_ts_str)

        # convert to local time
        local_tz = pytz.timezone('America/Chicago') # use your local timezone name here
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
        local_dt = local_tz.normalize(local_dt) # .normalize might be unnecessary
    else:
        local_dt = "UNK(%s)" % dts_ts

    return local_dt

def parse_timestamp(line):
    """ parse a timestamp from a line """
    timestamp = None
    lmatch = re.match(r'(\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d)', line)
    if lmatch:
        timestamp = lmatch.group(1)
    return timestamp

def get_last_timestamp_log_file(logfile):
    """ get the last timestamp from a log file """

    laststamp = 'N/A'
    if logfile is not None and os.path.exists(logfile):
        try:
            with open(logfile, "rb") as logfh:
                logfh.seek(-2, 2)            # Jump to the second last byte.
                while logfh.read(1) != "\n": # Until EOL is found...
                    logfh.seek(-2, 1)        # ...jump back the read byte plus one more.
                last = logfh.readline()      # Read last line.

                laststamp = parse_timestamp(last)
        except IOError:
            pass

    return laststamp


def get_latest_log_file(basedir):
    """ get the latest log filename """
    logfile = basedir

    if os.path.exists(basedir):
        # get max year
        years = os.listdir(basedir)
        logfile += '/'+max(years)

        # get max month
        months = os.listdir(logfile)
        logfile += '/'+max(months)

        # get max filename
        filenames = os.listdir(logfile)
        logfile += '/'+max(filenames)
    else:
        logfile = None


    return logfile

def get_last_handler_error(basedir):
    """ look for any errors in last two log files"""
    logfile = basedir
    lastlogfile = basedir

    years = os.listdir(basedir)
    logfile += '/'+max(years)

    months = os.listdir(logfile)
    logfile += '/'+max(months)

    filenames = os.listdir(logfile)
    logfile += '/'+max(filenames)

    if len(filenames) == 1:
        if len(months) == 1:
            lastlogfile += '/' + str((int(max(years)) - 1)) + '/12'
        else:
            lastlogfile += '/'+max(years)
            lastlogfile += '/%02d' % (int(max(months)) - 1)
        lfilenames = os.listdir(lastlogfile)
        lastlogfile += '/' + max(lfilenames)
    else:
        lastlogfile += '/' + max(years) + '/' + max(months)
        del filenames[filenames.index(max(filenames))]
        lastlogfile += '/' + max(filenames)

    cmd = "grep -EH 'error|exception|Error|Exception|ERROR|EXCEPTION' %s" % logfile
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = process.communicate()[0]

    cmd = "grep -EH 'error|exception|Error|Exception|ERROR|EXCEPTION' %s" % lastlogfile
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    lout = process.communicate()[0]

    outlines = out.split('\n')
    loutlines = lout.split('\n')
    loutlines.extend(outlines)
    loutlines = filter(None,loutlines)
    for i,val in enumerate(loutlines):
        loutlines[i] = val.replace(basedir + '/', '',1)
        loutlines[i] = loutlines[i].replace(':','||',1)
        loutlines[i] = loutlines[i].split('||')
    return loutlines

def get_timestamp_last_accept():
    """ get the timestamp of the last accept """
    logdir = ACCEPT_LOG_DIR
    latest_log = get_latest_log_file(logdir)
    last_accept = get_last_timestamp_log_file(latest_log)
    return last_accept

def get_timestamp_last_processed():
    """ get the timestamp of the last file processed """
    logdir = HANDLER_LOG_DIR

    latest_log = get_latest_log_file(logdir)

    out = ''
    if latest_log is not None:
        latest_log_dir = os.path.dirname(latest_log)
        filelist = glob.glob('%s/*.log' % latest_log_dir)
        #print filelist
        files = ' '.join(filelist)
        #print files

        last_line = ""
        cmd = 'grep -h handle_file %s' % files    # no filename
        if verbose:
            print cmd
        process = subprocess.Popen(cmd.split(), shell=False,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = process.communicate()[0]
    last_line = None
    if len(out) > 0:
        outlines = out.strip('\n').split('\n')
        if len(outlines) >= 1:
            last_line = outlines[-1]
    if verbose:
        print "last_line:", last_line

    last_processed = "N/A"
    if last_line is not None:
        last_processed = parse_timestamp(last_line)
    if verbose:
        print "last_processed:", last_processed

    return last_processed

def get_last_transfer_summary():
    """ get last transfer summary line from the dts log """
    dtslog = DELIVERY_LOG

    last_line = None
    out = ''
    if os.path.exists(dtslog):
        cmd = 'grep XSUM %s' % dtslog
        if verbose:
            print cmd
        process = subprocess.Popen(cmd.split(), shell=False,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = process.communicate()[0]

    if len(out) > 0:
        outlines = out.split('\n')
        if len(outlines) >= 2:
            last_line = outlines[-2]
        elif len(outlines) == 1:
            last_line = outlines[-1]
    if verbose:
        print "last_line:", last_line

    msg_str = ''
    if last_line is not None:
        tstamp = last_line[:13]
        local_ts = convert_dts_log_timestamp(tstamp)

        dts_msg = last_line[14:].strip()
        msg_str = "%s - %s" % (local_ts.strftime("%Y/%m/%d %H:%M:%S"), dts_msg)

    return msg_str

def get_last_dts_err():
    """ get the last error message from the dts log """
    dtslog = DELIVERY_LOG

    out = ''
    last_line = None
    if os.path.exists(dtslog):
        cmd = 'grep ERR %s' % dtslog
        if verbose:
            print cmd
        process = subprocess.Popen(cmd.split(), shell=False,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out = process.communicate()[0]

    if len(out) > 0:
        outlines = out.split('\n')
        if len(outlines) >= 2:
            last_line = outlines[-2]
        elif len(outlines) == 1:
            last_line = outlines[-1]

    if verbose:
        print "last_line:", last_line

    msg_str = ''
    if last_line is not None:
        tstamp = last_line[:13]
        local_ts = convert_dts_log_timestamp(tstamp)

        dts_msg = last_line[14:].strip()
        msg_str = "%s - %s" % (local_ts.strftime("%Y/%m/%d %H:%M:%S"), dts_msg)

    return msg_str


def get_nightly_summary_index():
    """ get the nite summary urls from the nite summary index page """
    nite_summary_urls = {}
    try:
        index_file = urllib2.urlopen(NITESUM_BASE+"index.html").read()
        if verbose:
            print 'index_file=', index_file
        for line in index_file.split('\n'):
            lmatch = re.search(r"nightsum-(\d\d\d\d)-(\d\d)-(\d\d)/nightsum.html", line)
            if lmatch:
                nite = "%s%s%s" % (lmatch.group(1), lmatch.group(2), lmatch.group(3))
                url = NITESUM_BASE+"nightsum-%s-%s-%s/nightsum.html" % (lmatch.group(1), lmatch.group(2), lmatch.group(3))
                nite_summary_urls[nite] = url
    except Exception:
        pass
    return nite_summary_urls


######################################################################
def print_exposure_html(timestamp, nitelist, info, reptype, cron=0):
    htmlfile = create_full_html_filename('dtsmonitor_exp_%s.html' % reptype)
    with open(htmlfile, 'w') as htmlfh:
        htmlfh.write("<html>\n")
        htmlfh.write("<body>\n")
        htmlfh.write("<p>\n")
        htmlfh.write("%s exposure report for %s nites generated at %s by user %s on %s<br>\n" % \
                 (reptype, len(nitelist), timestamp.strftime("%Y/%m/%d %H:%M:%S"),
                  getpass.getuser(), socket.gethostname()))
        tdelta = (datetime.now()-timestamp)
        htmlfh.write("(")
        if cron > 0:
            htmlfh.write("Running in cron every %s minutes.  " % cron)
        htmlfh.write("Took %0d.%04d secs to generate)<br>" % (tdelta.seconds, tdelta.microseconds))
        htmlfh.write("</p>\n")
        htmlfh.write("<table border=1 cellpad=3>\n")
        nitecnt = 0
        for nite in nitelist:
            explist = list(set(info['sispi_delivered'][nite].keys()) | set(info['sispi_not_delivered'][nite].keys()) | set(info['desdm'][nite].keys()) | set(info['decade'][nite].keys()))
            for expnum in sorted(explist, reverse=True):

                filename = 'DECam_%08d.fits.fz' % expnum
                if filename in info['failures'] or filename in info['decade_failures']:
                    expstate = 'fail'
                    if expnum in info['sispi_delivered'][nite]:
                        expinfo = info['sispi_delivered'][nite][expnum]
                    else:
                        expinfo = {}
                elif expnum not in info['desdm'][nite] and expnum not in info['decade'][nite]:
                    if expnum in info['sispi_delivered'][nite]:
                        expinfo = info['sispi_delivered'][nite][expnum]
                        expstate = 'missing'
                    elif expnum in info['sispi_not_delivered'][nite]:
                        expinfo = info['sispi_not_delivered'][nite][expnum]
                        if reptype == 'full':
                            expstate = 'not delivered'
                        else:
                            expstate = 'skip'
                elif expnum in info['desdm'][nite]:
                    expinfo = info['desdm'][nite][expnum]
                    if expnum in info['sispi_delivered'][nite]:
                        if reptype != 'short':
                            expstate = 'ok'
                        else:
                            expstate = 'skip'
                    elif expnum not in info['sispi_not_delivered'][nite]:
                        expstate = 'extra'
                elif expnum in info['decade'][nite]:
                    expinfo = info['decade'][nite][expnum]
                    if expnum in info['sispi_delivered'][nite]:
                        if reptype != 'short':
                            expstate = 'ok'
                        else:
                            expstate = 'skip'
                    elif expnum not in info['sispi_not_delivered'][nite]:
                        expstate = 'extra'

                if expstate != 'skip':
                    htmlfh.write("<tr>")
                    for k in ['obstype', 'filter', 'object', 'propid', 'proposer', 'program']:
                        if k not in expinfo or expinfo[k] is None:
                            expinfo[k]="&nbsp;"

                    if len(expinfo['object']) > 25:
                        expinfo['object'] = expinfo['object'][:24]
                    if len(expinfo['proposer']) > 15:
                        expinfo['proposer'] = expinfo['proposer'][:14]
                    if len(expinfo['program']) > 40:
                        expinfo['program'] = expinfo['program'][:39]

                    # nite, expnum, field, band, exptime, desdm, manifest_filename
                    nitecolor = NITE_COLORS[nitecnt % len(NITE_COLORS)]
                    htmlfh.write('<td style="background-color:%s">%s</td>' % (nitecolor, nite))
                    htmlfh.write("<td>%s</td>" % expnum)
                    htmlfh.write("<td>%s</td>" % expinfo['obstype'])

                    if 'band' not in expinfo or expinfo['band'] is None:
                        if expinfo['obstype'] in ['zero', 'calibration'] or expinfo['object'] in ['pointing', 'focus', 'donut']:
                            htmlfh.write("<td>&nbsp;</td>")
                        else:
                            htmlfh.write('<td style="background-color:orange">(filter: %s)</td>' % (expinfo['filter'].split()[0]))
                    else:
                        htmlfh.write("<td>%s</td>" % expinfo['band'])
                    htmlfh.write("<td>%s</td>" % expinfo['propid'])
                    htmlfh.write("<td>%s</td>" % expinfo['proposer'])
                    htmlfh.write("<td>%s</td>" % expinfo['object'])
                    htmlfh.write("<td>%s</td>" % expinfo['program'])


                    if expstate == 'fail':
                        htmlfh.write('<td style="background-color:red">%s</td>' % info['failures'][filename]['rejected_msg'])
                    elif expstate == 'missing':
                        htmlfh.write('<td style="background-color:orange">%s</td>' % 'Missing')
                    elif expstate == 'extra':
                        htmlfh.write('<td style="background-color:cyan">%s</td>' % 'Extra')
                    elif expstate == 'ok':
                        htmlfh.write('<td>&nbsp;</td>')
                    elif expstate == 'not delivered':
                        htmlfh.write('<td>delivered=False</td>')
                    else:
                        htmlfh.write('<td style="background-color:red">internal error</td>')
                    htmlfh.write("</tr>")

            nitecnt += 1
        htmlfh.write("</table>\n")
        htmlfh.write("</p>\n")
        htmlfh.write("</body>\n")
        htmlfh.write("</html>")



######################################################################
def print_sne_html(timestamp, nitelist, info, cron=0):

    htmlfile = create_full_html_filename('dtsmonitor_sne.html')
    desdm_info = info['desdm']
    html_info = info['sne']

    htmlfh = open(htmlfile, 'w')
    htmlfh.write("<html>\n")
    htmlfh.write('<title>SNe DTS monitor</title>\n')
    htmlfh.write("<body>\n")
    htmlfh.write("<p>\n")
    htmlfh.write("SNe DTS report for %s nites generated at %s by user %s on %s<br>\n" % \
             (len(nitelist), timestamp.strftime("%Y/%m/%d %H:%M:%S"),
              getpass.getuser(), socket.gethostname()))
    tdelta = (datetime.now()-timestamp)
    htmlfh.write("(")
    if cron > 0:
        htmlfh.write("Running in cron every %s minutes.  " % cron)
    htmlfh.write("Took %0d.%04d secs to generate)<br>" % (tdelta.seconds, tdelta.microseconds))
    htmlfh.write("</p>\n")
    htmlfh.write("Descriptions\n")
    htmlfh.write("<ul>\n")
    htmlfh.write("<li>All yellow line means that the line was skipped when ingesting manifest file</li>\n")
    htmlfh.write("<li>Orange manifest_filename means missing from DESDM archive (probably waiting for delivery)</li>\n")
    htmlfh.write("<li>Orange expnum means missing from DESDM archive (probably waiting for delivery if delivered column is True)</li>\n")
    htmlfh.write("<li>Only yellow delivered column means probably not going to get that exposure</li>\n")
    htmlfh.write("<li>Red means problem that needs to be investigated (usually mismatch between manifest file and exposure metadata)</li>\n")
    htmlfh.write("</ul>\n")

    htmlfh.write("<table border=1 cellpad=3>\n")

    htmlfh.write('<tr>\n')
    htmlfh.write('<th>nite</th>\n')
    htmlfh.write('<th>field</th>\n')
    htmlfh.write('<th>band</th>\n')
    htmlfh.write('<th>manifest filename</th>\n')
    htmlfh.write('<th>seq</th>\n')
    htmlfh.write('<th>expnum</th>\n')
    htmlfh.write('<th>exptime</th>\n')
    htmlfh.write('<th>delivered</th>\n')
    htmlfh.write('</tr>\n')
    for nite in nitelist:
        if nite in html_info and len(html_info[nite]) > 0:
            ndict = html_info[nite]
            for expnum, edict in sorted(ndict.items()):
                dmdict = None
                if expnum in desdm_info[nite]:
                    dmdict = desdm_info[nite][expnum]

                if edict['skip']:
                    htmlfh.write('<tr style="background-color:yellow">\n')
                else:
                    htmlfh.write('<tr>\n')
                htmlfh.write('<td>%s</td>\n' % nite)
                if dmdict is not None and edict['field'] != dmdict['field']:
                    htmlfh.write('<td style="background-color:red">%s/%s</td>\n' % (edict['field'], dmdict['field']))
                else:
                    htmlfh.write('<td>%s</td>\n' % edict['field'])

                if dmdict is not None and edict['band'] != dmdict['band']:
                    htmlfh.write('<td style="background-color:red">%s/%s</td>\n' % (edict['band'], dmdict['band']))
                else:
                    htmlfh.write('<td>%s</td>\n' % edict['band'])

                if edict['desdm_manifest'] == 'N':
                    htmlfh.write('<td style="background-color:orange">%s</td>\n' % edict['manifest'])
                else:
                    htmlfh.write('<td>%s</td>\n' % edict['manifest'])

                htmlfh.write('<td>%s/%s</td>\n' % (edict['seqnum'],edict['seqtot']))

                if dmdict is None:
                    htmlfh.write('<td style="background-color:orange">%s</td>' % edict['expnum'])
                else:
                    htmlfh.write('<td>%s</td>\n' % edict['expnum'])

                if dmdict is not None and edict['exptime'] != dmdict['exptime']:
                    htmlfh.write('<td style="background-color:red">%s/%s</td>\n' % (edict['exptime'], dmdict['exptime']))
                else:
                    htmlfh.write('<td>%s</td>\n' % edict['exptime'])

                if not edict['delivered']:
                    htmlfh.write('<td style="background-color:yellow">%s</td>\n' % edict['delivered'])
                else:
                    htmlfh.write('<td>%s</td>\n' % edict['delivered'])
                htmlfh.write('</tr>\n')

    htmlfh.write('</table>\n')
    htmlfh.write('</p>\n')
    htmlfh.write('</body>\n')
    htmlfh.write('</html>\n')

    htmlfh.close()




######################################################################
def print_summary_html(timestamp, nitelist, nite_info, extra_info, info, cron=0):
    """ print summary in html """

    htmlfile = create_full_html_filename('dtsmonitor.html')
    if verbose:
        print "Writing html to %s" % (htmlfile)
    with open(htmlfile, "w") as htmlfh:
        htmlfh.write("<html>\n")
        htmlfh.write("<body>\n")
        htmlfh.write("<p>\n")
        htmlfh.write("Report for %s nites generated at %s by user %s on %s<br>\n" % \
                 (len(nitelist), timestamp.strftime("%Y/%m/%d %H:%M:%S"),
                  getpass.getuser(), socket.gethostname()))
        tdelta = (datetime.now()-timestamp)
        htmlfh.write("(")
        if cron > 0:
            htmlfh.write("Running in cron every %s minutes.  " % cron)
        htmlfh.write("Took %0d.%04d secs to generate)<br>" % (tdelta.seconds, tdelta.microseconds))
        htmlfh.write("</p>\n")
        htmlfh.write("<p>\n")
        htmlfh.write("<h2>Lasts</h2>\n")
        htmlfh.write("<ul>\n")
        #htmlfh.write("<li>last time file delivered (accept script called): %s</li>\n" % \
        #         extra_info['last_accept'])
        htmlfh.write("<li>last time file processed (cron job ran with work to do): %s</li>\n" % \
                 extra_info['last_processed'])
        htmlfh.write("<li>number of files in DTS delivery directory (%s): %s</li>\n" % \
                 (DELIVERY_DIR, extra_info['num_delivery']))
        htmlfh.write("<li>last file processed error: %s</li>\n" % extra_info['last_processed_err'])
        #htmlfh.write("<li>last DTS transfer summary (seconds): %s</li>\n" % extra_info['last_xsum'])
        #htmlfh.write("<li>last DTS ERR message: %s</li>\n" % extra_info['last_dts_err'])
        htmlfh.write("</ul>\n")
        htmlfh.write("</p>\n")

        htmlfh.write("<p>\n")
        htmlfh.write("<h2>Other more detailed web pages</h2>\n")
        htmlfh.write("<ul>\n")
        #htmlfh.write('<li><a href="http://deslogin.cosmology.illinois.edu/~mgower/dtsmonitor_exp_short.html">Only differences by exposures</a>\n')
        #htmlfh.write('<li><a href="http://deslogin.cosmology.illinois.edu/~mgower/dtsmonitor_exp_normal.html">Normal list exposures</a>\n')
        #htmlfh.write('<li><a href="http://deslogin.cosmology.illinois.edu/~mgower/dtsmonitor_exp_full.html">Full list exposures</a>\n')
        #htmlfh.write('<li><a href="http://deslogin.cosmology.illinois.edu/~mgower/dtsmonitor_sne.html">SNe</a>\n')
        htmlfh.write('<li><a href="dtsmonitor_exp_short.html">Only differences by exposures</a>\n')
        htmlfh.write('<li><a href="dtsmonitor_exp_normal.html">Normal list exposures</a>\n')
        htmlfh.write('<li><a href="dtsmonitor_exp_full.html">Full list exposures</a>\n')
        htmlfh.write('<li><a href="dtsmonitor_sne.html">SNe</a>\n')
        htmlfh.write("</ul>\n")
        htmlfh.write("</p>\n")


        # column descriptions
        htmlfh.write("<p>\n")
        htmlfh.write("<h2>Nitely Counts</h2>\n")
        htmlfh.write("Column descriptions\n")
        htmlfh.write("<ul>\n")
        htmlfh.write("<li>Nite Sum: link appears if page exists for nite at %s</li>\n" % NITESUM_BASE)
        htmlfh.write("<li>SISPI: number of exposures in SISPI backup DB</li>\n")
        htmlfh.write("<li>DESDM: number of exposures in desoper DB (are officially in DESDM archive)</li>\n")
        htmlfh.write("<li>Missing: number of exposures in SISPI, but not in DESDM.<br>Only includes those that should be in DESDM (calibrations, des queue, SMASH project)</li>\n")
        htmlfh.write("<li>Extra: number of exposures in DESDM, but not in SISPI.<br>Same rules as for Missing</li>\n")
        htmlfh.write("<li>Manifest: suspect manifest file not transfered/ingested because missing exposures in manifest_exposure table (missing exposure count)</li>\n")
        htmlfh.write("</ul>\n")


        # table of information
        htmlfh.write("<table border=1 cellpad=3>\n")
        htmlfh.write("<tr><th>nite</th><th>nite sum</th><th>SISPI</th><th>DESDM</th><th>Failed</th><th>Missing</th><th>Extra</th><th>Manifest</th><th>Exposures</th></tr>\n")
        for nite in sorted(nitelist, reverse=True):
            htmlfh.write("<tr>")
            htmlfh.write("<td>%s</td>" % nite)
            if nite_info[nite]['nite_sum'] is None:
                htmlfh.write("<td>&nbsp;</td>")
            else:
                htmlfh.write('<td><a href="%s"</a>link</td>' % nite_info[nite]['nite_sum'])


            htmlfh.write("<td>%s</td>" % nite_info[nite]['cnt_sispi'])
            htmlfh.write("<td>%s</td>" % nite_info[nite]['cnt_desdm'])
            if nite_info[nite]['cnt_fail'] != 0:
                htmlfh.write('<td style="background-color:red">%s</td>' % nite_info[nite]['cnt_fail'])
            else:
                htmlfh.write("<td>%s</td>" % nite_info[nite]['cnt_fail'])
            if nite_info[nite]['cnt_missing'] != 0:
                htmlfh.write('<td style="background-color:orange">%s</td>' % nite_info[nite]['cnt_missing'])
            else:
                htmlfh.write("<td>%s</td>" % nite_info[nite]['cnt_missing'])
            if nite_info[nite]['cnt_extra'] != 0:
                htmlfh.write('<td style="background-color:cyan">%s</td>' % nite_info[nite]['cnt_extra'])
            else:
                htmlfh.write("<td>%s</td>" % nite_info[nite]['cnt_extra'])
            if nite_info[nite]['cnt_missing_manifest'] > 0:
                htmlfh.write('<td style="background-color:orange">%s</td>' % nite_info[nite]['cnt_missing_manifest'])
            else:
                htmlfh.write("<td>&nbsp;</td>")

            # last column with missing, failed, or extra expnums
            htmllines = []
            if nite_info[nite]['cnt_missing'] != 0:
                smdict = nite_info[nite]['summary_missing']
                for propid in smdict.keys():
                    for obstype in smdict[propid].keys():
                        htmllines.append('%s: %s: %s' % (propid, obstype, ', '.join([str(x) for x in smdict[propid][obstype]])))

            if nite_info[nite]['cnt_fail'] != 0:
                smdict = nite_info[nite]['summary_fail']
                for propid in smdict.keys():
                    for obstype in smdict[propid].keys():
                        htmllines.append("<font color='red'>%s: %s: %s</font>" % (propid, obstype, ', '.join([str(x) for x in smdict[propid][obstype]])))

            if nite_info[nite]['cnt_extra'] != 0:
                smdict = nite_info[nite]['summary_extra']
                #print '---------------------------------------------------\n\n\n',smdict,'\n'
                for propid in smdict.keys():
                    for obstype in smdict[propid].keys():
                        htmllines.append("<font color='cyan'>%s: %s: %s</font>" % (propid, obstype, ', '.join([str(x) for x in smdict[propid][obstype]])))


            if len(htmllines) > 0:
                htmlfh.write("<td>%s</td>" % '<br>'.join(htmllines))
            else:
                htmlfh.write("<td>&nbsp;</td>")



            htmlfh.write("</tr>\n")
        htmlfh.write("</table>\n")
        htmlfh.write("</p>\n")
        htmlfh.write("<p>\n")
        htmlfh.write("<h2>Last 20 failures within date range:</h2>\n")
        htmlfh.write("Data comes from table dts_bad_file\n")
        htmlfh.write("Column descriptions\n")
        htmlfh.write("<ul>\n")
        htmlfh.write("<li>rejected_date: timestamp when delivered file was rejected.  Orange if within last 3 days.</li>\n")
        htmlfh.write("<li>orig_filename: filename as delivered.  uniq_filename in table has timestamp added to allow for multiple failures for a single file.</li>\n")
        htmlfh.write("<li>rejected_msg: error message describing why the file was rejected</li>\n")
        htmlfh.write("</ul>\n")


        htmlfh.write("<table border=1 cellpad=3>\n")
        htmlfh.write("<tr><th>rejected_date</th><th>orig_filename</th><th>rejected_msg</th></tr>\n")
        sortfail = sorted(info['failures'].values(), key=lambda item: item['rejected_date'],
                          reverse=True)
        for fcnt in range(0, min(20, len(sortfail))):
            htmlfh.write("<tr>\n")
            if (timestamp.date() - timedelta(days=3)) <= sortfail[fcnt]['rejected_date'].date():
                htmlfh.write('<td style="background-color:orange">%s</td>\n' % \
                         sortfail[fcnt]['rejected_date'])
            else:
                htmlfh.write("<td>%s</td>\n" % sortfail[fcnt]['rejected_date'])
            htmlfh.write("<td>%s</td>\n" % sortfail[fcnt]['orig_filename'])
            htmlfh.write("<td>%s</td>\n" % sortfail[fcnt]['rejected_msg'])
            htmlfh.write("</tr>\n")
        htmlfh.write("</table>\n")
        htmlfh.write("</p>\n")

        htmlfh.write("<p>\n")
        htmlfh.write("<h2>Any ingest errors within last 2 log files</h2>\n")
        htmlfh.write("Data comes from log files in %s" % (HANDLER_LOG_DIR))
        htmlfh.write("<table border=1 cellpad=3>\n")
        htmlfh.write("<tr><th>File</th><th>Message</th></tr>\n")
        for item in info['ingest_errors']:
            htmlfh.write("<tr><td>%s</td><td>%s</td></tr>\n" % (item[0],item[1]))
        htmlfh.write("</table>\n")
        htmlfh.write("</p>\n")

        htmlfh.write("<p>\n")
        htmlfh.write("<h2>Count duplicate deliveries within date range:</h2>\n")
        htmlfh.write("Data comes from table dts_bad_file\n")
        htmlfh.write("<table border=1 cellpad=3>\n")
        htmlfh.write("<tr><th>nite</th><th>count</th></tr>\n")
        for nite in nitelist:
            if nite in info['duplicates'] and len(info['duplicates'][nite]) > 0:
                htmlfh.write("<tr>\n")
                htmlfh.write("<td>%s</td>\n" % nite)
                htmlfh.write("<td>%s</td>\n" % len(info['duplicates'][nite]))
                htmlfh.write("</tr>\n")
        htmlfh.write("</table>\n")
        htmlfh.write("</p>\n")

        htmlfh.write("</body>\n")
        htmlfh.write("</html>")


######################################################################
def parse_argv(argv):
    """ Return command line arguments as dict """
    parser = argparse.ArgumentParser(description='Compare exposure DB entries for DESDM vs SISPI backup')
    parser.add_argument('--cron', action='store')
    parser.add_argument('--des_services', action='store')
    parser.add_argument('--des_db_section', action='store', required=True)
    parser.add_argument('--sispi_db_section', action='store', required=True)
    parser.add_argument('--decade_db_section', action='store', required=True)
    parser.add_argument('--num_nites', type=int, action='store', required=False, default=DEFAULT_NUM_NITES)
    parser.add_argument('--nitestr', action='store')
    parser.add_argument('--verbose', action='store_true')
    args = vars(parser.parse_args(argv))   # convert dict
    return args

######################################################################
def convert_sispi_date_to_nite(sispidate):
    """ Convert sispi date to nite string """
    nitesplit = sispidate.replace(hour=15)
    expdate = sispidate
    if sispidate < nitesplit:
        expdate = sispidate - timedelta(days=1)
    expnite = expdate.strftime("%Y%m%d")

    return expnite

######################################################################
# SISPI db
def get_sispi_info(dbh, nitelist, propids):
    """ Returns list of expnums in sispi db exposure table for nites """

    #print "Querying SISPI for expnums"
    sispi_info = {}
    sispi_not_delivered = {}
    sispi_sne = {}
    for nite in nitelist:
        nitedate = datetime.strptime(nite, "%Y%m%d")
        #begdate = nitedate.replace(hour=9, tzinfo=pytz.timezone('UTC'))
        begdate = nitedate.replace(hour=15)
        begstr = begdate.strftime('%Y-%m-%d %H:%M')

        enddate = begdate + timedelta(days=1)
        endstr = enddate.strftime('%Y-%m-%d %H:%M')
        #print begdate
        #print enddate

        # removed dtsqueue='des' as keeping it assumes dtsqueue is never wrong
        #print nite,propids[nite]
        sql = "select id as expnum,propid,proposer,program,flavor as obstype,filter,object,date,seqid,seqnum,seqtot,exptime,delivered,dtsqueue from exposure where %s <= date and date <= %s and (flavor != 'object' or propid in ('%s'));" % (dbh.get_named_bind_string("begdate"), dbh.get_named_bind_string("enddate"), "','".join(propids[nite]))
        #print sql
        curs = dbh.cursor()
        curs.execute(sql, {'begdate':begdate, 'enddate':enddate})
        desc = [d[0].lower() for d in curs.description]

        sispi_info[nite] = {}
        sispi_sne[nite] = {}
        sispi_not_delivered[nite] = {}
        for row in curs:
            d = dict(zip(desc, row))
            d['nite'] = convert_sispi_date_to_nite(d['date'])
            if d['nite'] != nite:
                print "Warn: sispi nite mismatch", expnum
            d['band'] = None
            if d['filter'] is not None and d['filter'] != '':
                try:
                    d['band']=smfuncs.create_band(d['filter'])
                except:
                    #print "smfuncs failed", d['filter'], d['obstype']
                    pass
            if d['delivered']:
                sispi_info[d['nite']][d['expnum']] = d
            else:
                sispi_not_delivered[d['nite']][d['expnum']] = d

            # if a supernova exposure
            if d['obstype'] == 'object':
                m = re.search("DES supernova", d['object'])
                if m:
                    sispi_sne[nite][d['expnum']] = d
                    if d['seqid'] is not None:
                        d['manifest'] = '(from seqid) manifest_' + re.sub('[ :-]', '', d['seqid'])
                        if not d['seqid'].endswith('.json'):
                            d['manifest'] += '.json'
                    else:
                        d['manifest'] = 'unknown'
                    if object is not None:
                        try:
                            d['field'] = smfuncs.create_field(d['object'])
                        except:
                            d['field'] = 'unknown'
    #print sispi_info.keys()
    return sispi_info, sispi_not_delivered, sispi_sne


######################################################################
# DESDM db
def get_desdm_info(dbh, nitelist):
    """ Returns list of expnums in DESDM db exposure table for given list of nites """
    # assumes nitelist is in reverse order

    sql = "select expnum,nite,field,exptime,propid,proposer,program,obstype,band,object from exposure where nite=%s" % (dbh.get_named_bind_string('nite'))
    curs = dbh.cursor()
    #print "Querying DESDM for expnums"
    desdm_info = {}
    for nite in nitelist:
        desdm_info[nite] = {}

        curs.execute(sql, {'nite': nite})
        desc = [d[0].lower() for d in curs.description]

        for row in curs:
            d = dict(zip(desc, row))
            desdm_info[nite][d['expnum']]=d
        #print nite,len(desdm_info[nite])
    return desdm_info



def get_desdm_fail_by_nite(failures, sispi_info):
    # first make a dictionary by expnum
    # repeated failures don't count
    by_expnum = {}
    for faild in failures.values():
        if 'expnum' in faild:
            if faild['expnum'] not in by_expnum:
                by_expnum[faild['expnum']] = {}
            by_expnum[faild['expnum']][faild['rejected_date']] = faild
        #else:
        #    print "missing expnum: ", faild

    by_nite = {}
    for nite in sispi_info:
        by_nite[nite] = {}
        for expnum in sispi_info[nite]:
            if expnum in by_expnum:
                by_nite[nite][expnum] = by_expnum[expnum]
            elif int(expnum)==519365:
                print "why %s, %s not in by_expnum for nite %s" % (expnum, type(expnum), nite), by_expnum.keys()

    return by_nite


def get_desdm_fail_info(dbh, begdate):
    """ Returns list of failed files and messages during the time period """

    sql = "select orig_filename, rejected_msg, rejected_date from dts_bad_file where rejected_date >= %s" % dbh.get_named_bind_string('begdate')
    curs = dbh.cursor()
    curs.execute(sql, {'begdate': begdate})
    desc = [d[0].lower() for d in curs.description]

    des_fail = {}
    duplicates = {}
    for row in curs:
        rowd = dict(zip(desc, row))
        rejnite = rowd['rejected_date'].strftime('%Y%m%d')
        fmatch = re.match(r'DECam_(\d+).fits.fz', rowd['orig_filename'])
        if fmatch:   # note manifest files won't match
            rowd['expnum'] = int(fmatch.group(1))
            curs2 = dbh.cursor()
            curs2.execute('select expnum from exposure where expnum=%i' % rowd['expnum'])
            if len(curs2.fetchall()) > 0:
                continue
        if rowd['rejected_msg'].lower() == 'duplicate file':
            if rejnite not in duplicates:
                duplicates[rejnite] = []
            duplicates[rejnite].append(rowd)
        else:
            des_fail[rowd['orig_filename']] = rowd

    return des_fail, duplicates


def get_propids(dbh, nitelist):
    """ Returns a list of propids per nite for which DESDM should be receiving non-calibration exposures """

    # valid DES propid = '2013A-9999', '2012B-0005', '2012B-0001', '2013B-0440'
    results = {}
    if dbh is None:
        # no DESDM DB connection, hardcode list
        for nite in nitelist:
            results[nite] = ['2012B-0001','2012B-0003','2012B-0005','2012B-0416','2012B-0448','2012B-3001','2012B-3002','2012B-3003','2012B-3004','2012B-3005','2012B-3006','2012B-3007','2012B-3011','2012B-3012','2012B-3013','2012B-3014','2012B-3015','2012B-3016','2012B-9993','2012B-9999','2013A-0351','2013A-0360','2013A-0411','2013A-0704','2013A-0719','2013A-0741','2013A-9999','2013B-0440','2013B-0614','2014A-0634','2014A-0640','2014B-0404','2015A-0322','2015B-0187','2015B-0250','2015B-0305','2015B-0307','2015B-0607','2016A-0366', '2017B-0110','2019A-0305','2019A-0065','2019A-0205','2019A-0240','2018A-0242']
    else:
        sql = 'select propid from dts_propid where start_time <= %s and end_time >= %s' % (dbh.get_named_bind_string('begdate'), dbh.get_named_bind_string('enddate'))
        curs = dbh.cursor()
        for nite in nitelist:
            nitedate = datetime.strptime(nite, "%Y%m%d")
            #print "N: ",nitedate
            begdate = nitedate.replace(hour=12)
            #print "B ",begdate
            enddate = begdate + timedelta(days=1)
            #print "E ",enddate
            curs.execute(sql, {'begdate': begdate, 'enddate': enddate})
            results[nite] = []
            for row in curs:
                results[nite].append(row[0])
                #print "  R ",results[nite]
    return results


#def get_manifest_exposures(dbh, nitelist):
#    """ Returns a list of expnums per nite that appears in manifest files already ingested """
#
#    sql = "select * from manifest_exposure where nite=%s" % dbh.get_named_bind_string('nite')
#
#    curs = dbh.cursor()
#
#    results = {}
#    for nite in nitelist:
#        results[nite] = {}
#        curs.execute(sql, {'nite': nite})
#        desc = [d[0].lower() for d in curs.description]
#        for row in curs:
#            d = dict(zip(desc, row))
#            results[nite][d['expnum']] = d
#    return results

######################################################################
def get_desdm_manifests(dbh, nitelist):
    sql = 'select * from manifest_exposure where nite=%s' % dbh.get_named_bind_string('nite')
    curs = dbh.cursor()

    manifests = {}
    manexposures = {}
    for nite in nitelist:
        manifests[nite] = {}
        curs.execute(sql, {'nite': nite})
        desc = [d[0].lower() for d in curs.description]
        for row in curs:
            rowd = dict(zip(desc, row))
            manexposures[rowd['expnum']] = rowd

            if rowd['field'] not in manifests[nite]:
                manifests[nite][rowd['field']] = {}
            if rowd['band'] not in manifests[nite][rowd['field']]:
                manifests[nite][rowd['field']][rowd['band']] = set()
            manifests[nite][rowd['field']][rowd['band']].add(rowd['manifest_filename'])

    return manexposures, manifests

######################################################################
def mark_sne_skip(sispi_info):
    # first group by seqid
    byseqid = {}
    stot = {}
    for nite, ndict in sispi_info.items():
        for expnum, edict in ndict.items():
            if edict['seqid'] not in byseqid:
                byseqid[edict['seqid']] = {}
            byseqid[edict['seqid']][edict['expnum']] = edict
            stot[edict['seqid']] = edict['seqtot']    # assumes all seqtot are correct

    # process each seqid/manifest
    for seqid, sdict in byseqid.items():
        byseqnum = [None] * (stot[seqid] + 1)
        for expnum, edict in sorted(sdict.items()):
            edict['skip'] = False
            if byseqnum[edict['seqnum']] is None:
                byseqnum[edict['seqnum']] = edict
            else:
                byseqnum[edict['seqnum']]['skip'] = True
                byseqnum[edict['seqnum']] = edict

def get_missing_manifest(nitelist, sispi_sne, manifest_exposures):
    """ determine if missing a manifest files for each nite """

    results = {}
    for nite in nitelist:
        #sispi_expnums = set(sispi_sne[nite].keys())
        #manifest_expnums = set(manifest_exposures[nite].keys())
        #results[nite] = len(sispi_expnums - manifest_expnums)
        results[nite] = []
    return results


def gather_lasts():
    """ Gather information about last events """

    extra_info = {}
    #extra_info['last_accept'] = get_timestamp_last_accept()
    extra_info['last_processed'] = get_timestamp_last_processed()
    #extra_info['last_xsum'] = get_last_transfer_summary()
    #extra_info['last_dts_err'] = get_last_dts_err()
    extra_info['num_delivery'] = count_num_files_in_delivery_dir()
    return extra_info


def summarize_expnums(explist, expinfo, dexpinfo={}):

    # subgroup by propid, obstype

    expgroups = {}
    for expnum in explist:
        if expnum in expinfo:
            edict = expinfo[expnum]
        else:
            edict = dexpinfo[expnum]
        if edict['propid'] not in expgroups:
            expgroups[edict['propid']] = {}
        if edict['obstype'] not in expgroups[edict['propid']]:
            expgroups[edict['propid']][edict['obstype']] = []
        expgroups[edict['propid']][edict['obstype']].append(expnum)

    return expgroups




def summarize_info(nitelist, info):

    summary_misc = gather_lasts()
    summary_nite = {}

    if len(info['failures']) > 0:
        lasterr = sorted(info['failures'].values(), key=lambda x:x['rejected_date'])[-1]
        summary_misc['last_processed_err'] = "%s - %s - %s" % (lasterr['rejected_date'].strftime("%Y/%m/%d %H:%M:%S"),
                                                               lasterr['orig_filename'], lasterr['rejected_msg'])
    else:
        summary_misc['last_processed_err'] = ""

    fail_by_nite = get_desdm_fail_by_nite(info['failures'], info['sispi_delivered'])

    #print sorted(desdm_info.keys())
    #print sorted(sispi_info.keys())

    summary_misc['extra'] = set()
    for nite in nitelist:
        summary_nite[nite] = {}

        if nite in info['nite_summary_urls']:
            summary_nite[nite]['nite_sum'] = info['nite_summary_urls'][nite]
        else:
            summary_nite[nite]['nite_sum'] = None

        if nite in info['sispi_delivered']:
            summary_nite[nite]['cnt_sispi'] = len(info['sispi_delivered'][nite])
        else:
            summary_nite[nite]['cnt_sispi'] = 0

        if nite in info['desdm']:
            summary_nite[nite]['cnt_desdm'] = len(info['desdm'][nite])
            #print nite, len(info['desdm'][nite]), len(info['decade'][nite]), len(info['desdm'][nite])+ len(info['decade'][nite])
        else:
            summary_nite[nite]['cnt_desdm'] = 0
        if nite in info['decade']:
            summary_nite[nite]['cnt_desdm'] += len(info['decade'][nite])

        # don't count as extra if we should have gotten it, but sispi.exposure.delivered was False
        extra_expnums = set(info['desdm'][nite].keys()) - set(info['sispi_delivered'][nite].keys()) - set(info['sispi_not_delivered'][nite].keys())
        #print "\nEXTRA "
        #print "\n       ", extra_expnums
        #print "\n  DES  ", info['desdm'][nite].keys()
        #print "\n  DEC  ", info['decade'][nite].keys()
        #print "\n  SIS  ", info['sispi_delivered'][nite].keys()
        summary_nite[nite]['cnt_extra'] = len(extra_expnums)
        summary_nite[nite]['summary_extra'] = summarize_expnums(extra_expnums, info['desdm'][nite], info['decade'][nite])
        summary_misc['extra'].union(extra_expnums)

        missing_expnums = set(info['sispi_delivered'][nite].keys()) - set(info['desdm'][nite].keys()) - set(fail_by_nite[nite].keys()) - set(info['decade'][nite].keys())
        summary_nite[nite]['cnt_missing'] = len(missing_expnums)
        summary_nite[nite]['summary_missing'] = summarize_expnums(missing_expnums, info['sispi_delivered'][nite])

        summary_nite[nite]['cnt_fail'] = len(fail_by_nite[nite])
        summary_nite[nite]['summary_fail'] = summarize_expnums(fail_by_nite[nite].keys(), info['sispi_delivered'][nite])

        if nite in info['missing_manifest']:
            summary_nite[nite]['cnt_missing_manifest'] = len(info['missing_manifest'][nite])
        else:
            summary_nite[nite]['cnt_missing_manifest'] = 0

    return summary_nite, summary_misc




def gather_info(args, nitelist):
    """ Workflow for gathering all the information """

    all_nitely_info = {}
    info = {}


    begnite = nitelist[-1]
    begdate = datetime.strptime(begnite, "%Y%m%d")

    info['nite_summary_urls'] = get_nightly_summary_index()

    try:
        #print "DESDM ------------------"
        # open DESDM connection here as needed multiple places
        dbh = desdbi.DesDbi(args['des_services'], args['des_db_section'], True)
        propids = get_propids(dbh, nitelist)
        info['desdm'] = get_desdm_info(dbh, nitelist)
        (info['failures'], info['duplicates']) = get_desdm_fail_info(dbh, begdate)
        (info['manifest_exposures'], info['manifests']) = get_desdm_manifests(dbh, nitelist)
        dbh.close()
    except:
        raise
        desdm_db_failure = True
        propids = get_propids(None, nitelist)
        info['desdm'] = search_disk_for_exposures(nitelist)
        info['failures'] = search_disk_for_failures(nitelist)   # does this handle duplicates separate from failures?

    try:
        #print "DECADE -----------------"
        dbh = desdbi.DesDbi(args['des_services'], args['decade_db_section'], True)
        info['decade'] = get_desdm_info(dbh, nitelist)
        (info['decade_failures'], info['decade_duplicates']) = get_desdm_fail_info(dbh, begdate)
        dbh.close()
    except:
        raise

    try:
        dbh = desdbi.DesDbi(args['des_services'], args['sispi_db_section'], True)
        info['sispi_delivered'], info['sispi_not_delivered'], info['sispi_sne'] = get_sispi_info(dbh, nitelist, propids)
        info['missing_manifest'] = get_missing_manifest(nitelist, info['sispi_sne'], info['manifest_exposures'])
        info['ingest_errors'] = get_last_handler_error(HANDLER_LOG_DIR)
    except:
        sispi_db_failure = True
        raise

    return info


def search_disk_for_exposures(nitelist):
    """ Get exposure filenames from disk """

    results = {}
    for nite in nitelist:
        results[nite] = {}
        nitepath = "%s/%s" % (dts_archive_path, nite)
        for (dirpath, dirnames, filenames) in os.walk(nitepath):
            for fname in filenames:
                m = re.match('DECam_(\d+).fits.fz', fname)
                if m:
                    expnum = int(m.group(1))
                    results[nite][expnum] = {'expnum': expnum, 'filename': os.basename(fname), 'fullname': fname}
    return results


def search_disk_for_failures(nitelist):
    # todo limit directory walking to time of nitelist
    results = {}
    for (dirpath, dirnames, filenames) in os.walk(FAIL_DIR):
        for fname in filenames:
            #print fname
            pass
    return results




def create_full_html_filename(htmlfilename):
    """ create the full filename for the output html file """

    if 'DTSMONITOR_OUTBASE' in os.environ:
        output_base = os.environ['DTSMONITOR_OUTBASE']
        monitor_html = "%s/%s" % (output_base, htmlfilename)
    elif 'USER' in os.environ:
        user = os.environ['USER']
        if user == 'databot2':
            output_base = "/work/QA/technical/dts"
        else:
            output_base = "/home/%s/public_html" % (user)
        monitor_html = "%s/%s" % (output_base, htmlfilename)
    else:
        print "Warning:  writing to current directory"
        monitor_html = "%s" % (htmlfilename)

    return monitor_html


def main():
    """ Program entry point """
    starttime = datetime.now()
    args = parse_argv(sys.argv[1:])

    if args['verbose'] is not None:
        verbose = args['verbose']

    endnite = args['nitestr']
    if endnite is None:
        enddate = datetime.now()
        endnite = enddate.strftime("%Y%m%d")
    else:
        enddate = datetime.strptime(endnite, "%Y%m%d")

    datelist = [enddate - timedelta(days=x) for x in range(0, args['num_nites'])]
    if verbose:
        print datelist
    nitelist = [datetime.strftime(x, "%Y%m%d") for x in datelist]

    info = gather_info(args, nitelist)
    summary_nite, summary_misc = summarize_info(nitelist, info)

    print_summary_html(starttime, nitelist, summary_nite, summary_misc, info, args['cron'])
    print_exposure_html(starttime, nitelist, info, 'short', args['cron'])
    print_exposure_html(starttime, nitelist, info, 'normal', args['cron'])
    print_exposure_html(starttime, nitelist, info, 'full', args['cron'])

    # sne monitoring turned off 10/23/2018
    #mark_sne_skip(info['sispi_sne'])
    #info['sne'] = combine_sne_info(nitelist, info)
    #print_sne_html(starttime, nitelist, info, args['cron'])

if __name__ == "__main__":
    main()
