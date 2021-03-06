#!/bin/sh

BASELOGDIR=/local/dts_desdm/logs/dts_file_handler_logs
SVNDIR=/local/dts_desdm/svncheckout

themonth=`/bin/date +%Y/%m`
logdir=$BASELOGDIR/$themonth
mkdir -p $logdir

thedate=`/bin/date +%Y%m%d`
logfile=$logdir/$thedate-handle_file_from_dts.log

echo `/bin/date` " - " $$ " BEG ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" >> $logfile
export DES_DB_SECTION=db-databot

unset EUPS_DIR
unset EUPS_PATH

echo `/bin/date` " - " $$ " eups setup main stack " >> $logfile 2>&1
#source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh 
source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh 
setup --nolocks Y2Nstack 1.0.6+0 >> $logfile 2>&1

# dtsfilereceiver checkout from svn
echo `/bin/date` " - " $$ " eups setup dtsfilereceiver " >> $logfile 2>&1
cd /home/databot2/dtsfilereceiver/trunk
setup --nolocks -j -r . >> $logfile 2>&1

#echo $DTSFILERECEIVER_DIR
echo `/bin/date` " - " $$ " running dts_file_handler.py " >> $logfile 2>&1
dts_file_handler.py --config $DTSFILERECEIVER_DIR/etc/dts_prod.cfg >> $logfile 2>&1
filehandstat=$?
echo `/bin/date` " - " $$ " dts_file_handler.py exitcode" $filehandstat >> $logfile 2>&1
