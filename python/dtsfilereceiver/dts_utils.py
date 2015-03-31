#!/usr/bin/env python

import datetime
import pytz
from dateutil.parser import parse

def convert_UTCstr_to_nite(datestr):
    # e.g. datestr: 2014-08-15T17:31:02.416533+00:00
    nite = None

    # convert date string to datetime object
    utc_dt = parse(datestr)

    # convert utc to local on mountain
    local_tz = pytz.timezone('Chile/Continental') # use your local timezone name here
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    local_dt = local_tz.normalize(local_dt) # .normalize might be unnecessary

    # see if before or after noon on mountain
    noon_dt = local_dt.replace(hour=12, minute=0, second=0, microsecond=0)
    if local_dt < noon_dt:  # if before noon, observing nite has previous date
        obsdate = (local_dt - datetime.timedelta(days=1)).date()
    else:
        obsdate = local_dt.date()

    nite = obsdate.strftime('%Y%m%d')
    return nite


def determine_filetype(filename):
    """ Returns the filetype of the given file or None if cannot determine filetype """
    filetype = None

    if filename.endswith('.fits'):
        filetype = 'raw'
    elif filename.startswith('manifest_SN') and filename.endswith('.json'):
        filetype = 'snmanifest'

    return filetype


def check_already_registered(filename, filemgmt):
    """ Throws exception if file is already registered """

    has_meta = filemgmt.file_has_metadata([filename])
    return len(has_meta) == 1


if __name__ == '__main__':
    pass
