"""
$ python search_wrapper.py config/cfg_file.json
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
    FILE_DIR = '/Users/jferrandiz/Code/splunk-sdk-python'
PROJ_DIR = os.path.dirname(FILE_DIR)
CODE_DIR = os.path.dirname(PROJ_DIR)
sys.path.insert(0, PROJ_DIR)
sys.path.insert(0, CODE_DIR)

from Utilities import time_utils as tu
import search as sp


def sp_wrapper(f_cfg):
    with open(f_cfg, 'r') as fp:
        d_cfg = json.load(fp)

    yy, mm, dd = d_cfg.get('year', None), d_cfg.get('month', None), d_cfg.get('day', None)

    # set date (yy, dd, mm) to yesterday UTC if missing
    if yy is None or mm is None or dd is None:  # default to yesterday
        date = tu.get_date(days_back=1, date_format='%Y-%m-%d', tz_str='UTC')
    else:
        mm = str(mm) if mm > 9 else '0' + str(mm)
        dd = str(dd) if dd > 9 else '0' + str(dd)
        date = str(yy) + '-' + str(mm) + '-' + str(dd)
    w_day = tu.get_from_str_date(date, date_format='%Y-%m-%d', what='weekday')

    if w_day < 5:  # do weekdays only
        yy, mm, dd = date.split('-')
        d_cfg['year'] = int(yy)
        d_cfg['day'] = int(dd)
        d_cfg['month'] = int(mm)
        my_log.info('launching ' + f_cfg + ' for date ' + date)
        sp.splunk_search(f_cfg, d_cfg)
    else:
        my_log.info('[' + __name__ + '] wrapper: no job to launch for ' + f_cfg + ' on ' + date)

if __name__ == '__main__':
    my_log.info('Search Wrapper started')
    if len(sys.argv) != 2:
        my_log.info('invalid parameters' + str(sys.argv))
        sys.exit(0)
    sp_wrapper(sys.argv[1])
