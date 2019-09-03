#!/bin/sh

unset EUPS_DIR
unset EUPS_PATH

source /work/apps/RHEL6/dist/eups/desdm_eups_setup.sh
setup --nolocks despydb 2.0.0+2
setup --nolocks psycopg2 2.4.6+2
setup --nolocks pytz 2013.7+3
setup --nolocks dateutil 1.5+3

# until a formal dtsfilereceiver package is made
cd /local/dts_desdm/svncheckout/dtsfilereceiver/branches/wrapper-refact
setup --nolocks -j -r . 
$DTSFILERECEIVER_DIR/bin/dts_monitor.py --des_db_section db-databot --sispi_db_section db-sispi $@
