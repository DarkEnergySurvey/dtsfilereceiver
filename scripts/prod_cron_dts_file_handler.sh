#!/bin/sh

BASELOGDIR=/home/databot2/logs/dts_file_handler_logs

themonth=`/bin/date +%Y/%m`
logdir=$BASELOGDIR/$themonth
mkdir -p $logdir

thedate=`/bin/date +%Y%m%d`
logfile=$logdir/$thedate-handle_file_from_dts.log

echo `/bin/date` "BEG ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" >> $logfile
export DES_DB_SECTION=db-databot

unset EUPS_DIR
unset EUPS_PATH

source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh 
setup --nolocks Y2Nstack 1.0.0+0 >> $logfile 2>&1

# dtsfilereceiver checkout from svn
cd /home/databot2/dtsfilereceiver/trunk
setup --nolocks -r . >> $logfile 2>&1

#echo $DTSFILERECEIVER_DIR
dts_file_handler.py --config $DTSFILERECEIVER_DIR/etc/dts_databot.cfg >> $logfile 2>&1
echo "dts_file_handler.py exitcode" $?
echo `/bin/date` "END ############################################################" >> $logfile
