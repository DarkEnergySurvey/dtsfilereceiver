#!/bin/sh

BASELOGDIR=/cluster_scratch/users/mgower/test_dts/dts_file_handler_logs

themonth=`/bin/date +%Y/%m`
logdir=$BASELOGDIR/$themonth
mkdir -p $logdir

thedate=`/bin/date +%Y%m%d`
logfile=$logdir/$thedate-handle_file_from_dts.log

echo `/bin/date` "BEG ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" >> $logfile
export DES_DB_SECTION=db-oracle-refact

unset EUPS_DIR
unset EUPS_PATH

source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh 
#export EUPS_PATH=/work/users/mgower/my_eups_prods:$EUPS_PATH
#setup --nolocks FWRefact mmg1.0 >> $logfile 2>&1
setup --nolocks Y2Nstack 1.0.0+0 >> $logfile 2>&1

cd /home/mgower/dts/dtsfilereceiver
setup --nolocks -r . >> $logfile 2>&1

#echo $DTSFILERECEIVER_DIR
dts_file_handler.py --config $DTSFILERECEIVER_DIR/etc/dts_mgower.cfg >> $logfile 2>&1
echo "dts_file_handler.py exitcode" $?
echo `/bin/date` "END ############################################################" >> $logfile
