""" Utility classes and functions for decadOAe file processing
"""

#import glob
#import shutil
#import json
#import sys
#import os
#import logging
import datetime
#from logging.handlers import TimedRotatingFileHandler
#import dateutil.parser as dparser

#from globus_sdk import (NativeAppAuthClient, TransferClient, AuthClient,
#                        RefreshTokenAuthorizer, TransferData)
#from globus_sdk.exc import GlobusAPIError

#import despydmdb.desdmdbi as desdmdbi
#import dtsfilereceiver.dts_utils as dtsutils


def to_night(night):
    year = int(night[:4])
    month = int(night[4:6])
    day = int(night[6:])
    return datetime.date(year,month,day)



