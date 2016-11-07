"""
$ python looper.py start_day end_day config/cfg_file.json
updates the day to yesterday in cfg file if day is null
"""

import sys
import json
import os
import logging

my_log = logging.getLogger(__name__)
my_log.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
for h in [sys.stdout]:  # sys.stderr redirected at output
    ch = logging.StreamHandler(h)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    my_log.addHandler(ch)

try:
    FILE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    FILE_DIR = '/Users/jferrandiz/Code/data_collection'
PROJ_DIR = os.path.dirname(FILE_DIR)
CODE_DIR = os.path.dirname(PROJ_DIR)
sys.path.insert(0, PROJ_DIR)
sys.path.insert(0, CODE_DIR)

from Utilities import time_utils as tu
import search as sp


def sp_looper(f_cfg, start_date, end_date):
    with open(f_cfg, 'r') as fp:
        d_cfg = json.load(fp)

    start_ts = tu.to_timestamp(start_date, date_format='%Y-%m-%d', tz_str='UTC')
    end_ts = tu.to_timestamp(end_date, date_format='%Y-%m-%d', tz_str='UTC') + 86400
    ts = start_ts
    while ts < end_ts:
        date = tu.to_date(ts, date_format='%Y-%m-%d', tz_str='UTC')
        w_day = tu.get_from_str_date(date, date_format='%Y-%m-%d', what='weekday')
        if w_day < 5:               # do weekdays only
            yy, mm, dd = date.split('-')
            d_cfg['year'] = int(yy)
            d_cfg['day'] = int(dd)
            d_cfg['month'] = int(mm)
            my_log.info('launching ' + f_cfg + ' for date ' + date + ' pid: ' + str(os.getpid()))
            sp.splunk_search(f_cfg, d_cfg)
        else:
            my_log.info('[' + __name__ + '] wrapper: no job to launch for ' + f_cfg + ' on ' + date + ' pid: ' + str(os.getpid()))
        ts += 86400

if __name__ == '__main__':
    my_log.info('Search Wrapper started')
    if len(sys.argv) != 4:
        my_log.info('invalid parameters' + str(sys.argv) + ' pid: ' + str(os.getpid()))
        sys.exit(0)
    sp_looper(sys.argv[3], sys.argv[1], sys.argv[2])
