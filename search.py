"""
$ python config/cfg_file.json
The cfg file contains information about the query to run including:
- date
- start and end hour
- output groupby
- log lines
- results are saved by pod, hourly as a csv zipped file and a specified directory of the form <top_dir>/yy/mm-dd/hh/<f_name>-<pod>-<hour>.csv.gz
This search function is robust in the sense that it will catch server disconnects and restart where it left it.
It also makes sure that all the records of the search are returned rather than the first 50K records.
The user must have a .splunkrc file.

"""

import sys
import json
import os
import gzip
import time
import logging
import itertools
from socket import error as SocketError
from time import sleep
import errno
import getpass


my_log = logging.getLogger(__name__)
my_log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
for h in [sys.stdout]:   # sys.stderr redirected at output
    ch = logging.StreamHandler(h)
    ch.setLevel(logging.DEBUG)
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

from splunklib.binding import HTTPError
import splunklib.client as client
import utils.__init__ as ut
import data_collection.utilities.dirs as dut
from Utilities import parallel as pl

FILE_DIR = '/Users/jferrandiz/Code/dashboard_creation_tool'
PROJ_DIR = os.path.dirname(FILE_DIR)
CODE_DIR = os.path.dirname(PROJ_DIR)
sys.path.insert(0, PROJ_DIR)
sys.path.insert(0, CODE_DIR)

if getpass.getuser() == 'jferrandiz':
    from MyUtilities import otp
    creds = otp.sso('/Users/jferrandiz/.credentials/aloha_secret.json')
    from MyUtilities import sfm_on as sfm_on
else:
    creds = None


# from dashboard_creation_tool.lib.util import InitiateProxySession

MAX_TRIES = 20     # number of tries after connection resets
POD_MAX = 1200.0   # max time per pod per hour
LOG_QUERY = False  # print query in log
VERBOSE = 0


def cmdline(argv, flags, **kwargs):
    """
    A cmdopts wrapper that takes a list of flags and builds the corresponding cmdopts rules to match those flags.
    """
    rules = dict([(flag, {'flags': ["--%s" % flag]}) for flag in flags])
    return ut.parse(argv, rules, ".splunkrc", **kwargs)


def track_job(job, verbose, pod):
    """
    loops and waits for the job to be done
    """
    while True:
        while not job.is_ready():
            pass
        stats = {'isDone': job['isDone'],
                 'doneProgress': job['doneProgress'],
                 'scanCount': job['scanCount'],
                 'eventCount': job['eventCount'],
                 'resultCount': job['resultCount']}
        progress = float(stats['doneProgress'])*100
        scanned = int(stats['scanCount'])
        matched = int(stats['eventCount'])
        results = int(stats['resultCount'])
        if verbose > 0:
            status = ("\r%s: %03.1f%% | %d scanned | %d matched | %d results" % (pod, progress, scanned, matched, results))
            my_log.debug(status)
        if stats['isDone'] == '1':
            break
        sleep(5)
    return


FLAGS_TOOL = ['verbose']

FLAGS_CREATE = [
    'earliest_time', 'latest_time', 'now', 'time_format',
    'exec_mode', 'search_mode', 'rt_blocking', 'rt_queue_size',
    'rt_maxblocksecs', 'rt_indexfilter', 'id', 'status_buckets',
    'max_count', 'max_time', 'timeout', 'auto_finalize_ec', 'enable_lookups',
    'reload_macros', 'reduce_freq', 'spawn_process', 'required_field_list',
    'rf', 'auto_cancel', 'auto_pause',
]

FLAGS_RESULTS = [
    'offset', 'count', 'search', 'field_list', 'f', 'output_mode'
]


def get_outfile(f_name, dir_name, pod, yy, mm, dd, start_hr, end_hr=None):
    yy, mm, dd = dut.set_data_dirs(yy + '-' + mm + '-' + dd)
    f_path = dir_name + '/' + yy + '/' + mm + '-' + dd + '/'
    hr_str = str(start_hr) if start_hr > 9 else '0' + str(start_hr)
    if end_hr is None:
        f_path += '/' + hr_str
        try:
            os.makedirs(f_path, 0755)
        except OSError:
            pass
        f_path += '/' + f_name + '-' + pod + '-' + hr_str + '.csv.gz'
    else:
        hr_str2 = str(end_hr) if end_hr > 9 else '0' + str(end_hr)
        f_path += '/' + f_name + '-' + pod + '-' + hr_str + '-' + hr_str2 + '.csv.gz'
    return f_path


def qry_check(service, yy, mm, dd, hr, pod, q_tail):
    time_str = 'earliest=' + splunk_date(yy, mm, dd, hr) + ' latest=' + splunk_date(yy, mm, dd, hr + 1)
    q = 'search index=' + pod + '\n\t' + time_str + '\n\t' + q_tail
    try:
        service.parse(q, parse_only=True)
        if LOG_QUERY is True:
            my_log.info('splunk query: ' + q)
        return q
    except HTTPError as e:
        my_log.info('query ' + q + ' is invalid: ' + e.message)
        sys.exit(2)


def execute_hr_job(q_tail, pod, service, kwargs_create, kwargs_results, f_out, dir_out, yy, mm, dd, hr):
    q = qry_check(service, yy, mm, dd, hr, pod, q_tail)
    # kwargs_create['adhoc_search_level'] = 'fast'  # fast, smart, verbose
    job = service.jobs.create(q, **kwargs_create)
    track_job(job, VERBOSE, pod + '::' + str(hr))   # DO NOT REMOVE: waits for the job to be done!!!!

    # offset loop to collect results with more than 50K records
    results_list = []                     # collect all results in a list
    kwargs_results['offset'] = 0          # always reset offset before staring the loop
    t_start = time.time()
    while kwargs_results['offset'] < int(job.resultCount):
        this_iter = job.results(**kwargs_results).read().split('\n')[:-1]  # drop last line as it is empty
        inc_list = this_iter if len(results_list) == 0 else this_iter[1:]  # drop header after first loop
        results_list += inc_list                                           # append the new results
        kwargs_results['offset'] += kwargs_results['count']                # shift offset
        if time.time() - t_start > POD_MAX:
            my_log.debug('ERROR:: could not complete pod: ' + pod + ', hour: ' + str(hr) + ' on time')
            break

    my_log.debug('pod: ' + pod + ', hour: ' + str(hr) + ' total records: ' + str(job.resultCount) + ' ' + ' collected records: ' + str(len(results_list) - 1))  # hdr + data
    if len(results_list) - 1 != int(job.resultCount):
        my_log.debug('ERROR::::pod: ' + pod + ', hour: ' + str(hr) + ' total records: ' + str(job.resultCount) + ' ' + ' collected records: ' + str(len(results_list) - 1))
    job.cancel()

    f_path = get_outfile(f_out, dir_out, pod, yy, mm, dd, hr)
    with gzip.open(f_path, 'w') as fp:
        if len(results_list) > 0:
            for s in results_list:
                fp.write(s + '\n')
            my_log.info('pod: ' + pod + ', hour: ' + str(hr) + ': ' + str(len(results_list)) + ' data records saved to ' + f_path)
        else:
            fp.write('')
            my_log.debug('WARNING:::pod: ' + pod + ', hour: ' + str(hr) + ': no results to save')


def stream_hr_job(q_tail, pod, service, kwargs_export, kwargs_results, f_out, dir_out, yy, mm, dd, hr):
    q = qry_check(service, yy, mm, dd, hr, pod, q_tail)
    kwargs_export["search_mode"] = 'normal'
    kwargs_export["output_mode"] = 'csv'
    # kwargs_export['output_time_format'] = '%Y-%m-%d %H:%M:%S'
    # kwargs_results = ut.dslice(opts.kwargs, FLAGS_RESULTS)
    # kwargs_results['output_time_format'] = '%Y-%m-%d %H:%M:%S'
    # kwargs_results['output_mode'] = 'csv'
    # kwargs_results['count'] = 50000
    # kwargs_results['offset'] = 0
    num_attempts, sleep_time, results = 0, 300, []

    while True:
        try:
            num_attempts += 1
            my_log.debug("Running (blocking) query... (attempt: %d)" % num_attempts)
            results = service.jobs.export(q, **kwargs_export)
            break
        except HTTPError as e:
            my_log.debug(e)
            if num_attempts > 10:
                my_log.error("Error: Export query failed %d times. Exiting now." % num_attempts)
                exit(3)
            time.sleep(sleep_time)

    if len(results) > 0:
        f_path = get_outfile(f_out, dir_out, pod, yy, mm, dd, hr)
        my_log.info('pod: ' + pod + ', hour: ' + str(hr) + ': ' + str(len(results)) + ' data records to ' + f_path + ' results type: ' + str(type(results)))
        with gzip.open(f_path, 'w') as fp:
            for s in results:
                fp.write(s + '\n')
        my_log.info('pod: ' + pod + ', hour: ' + str(hr) + ': ' + str(len(results)) + ' data records saved to ' + f_path)
    else:
        my_log.debug('WARNING:::pod: ' + pod + ', hour: ' + str(hr) + ': no results to save')

    if hasattr(results, "close"):
        results.close()
    return


def pod_apt(work, q_tail, kwargs_create, kwargs_results, kwargs_splunk, f_out, dir_out, yy, mm, dd, creds_file, str_mode, start_time, try_ctr):
    # one hr for pod
    pod, hr = work
    start_t = time.time()  # pod hr start
    my_log.debug('pod ' + pod + ' started for hour ' + str(hr))
    try:
        if time.time() - start_time > 86400.0:
            my_log.info('Exiting. @@@@@Process running for too long\n\n')
            sys.exit(0)

        service = client.connect(**kwargs_splunk)
        w_t = time.time()
        if str_mode is True:
            stream_hr_job(q_tail, pod, service, kwargs_create, kwargs_results, f_out, dir_out, yy, mm, dd, hr)
        else:
            execute_hr_job(q_tail, pod, service, kwargs_create, kwargs_results, f_out, dir_out, yy, mm, dd, hr)

        my_log.debug('pod: ' + pod + ', hour: ' + str(hr) + ' waited for: ' + str(int(w_t - start_t)) + ' secs and executed in ' + str(int(time.time() - w_t)) + ' secs')

    except TypeError:
        my_log.debug(':::WARNING::: job for ' + pod + ' and hour ' + str(hr) + ' failed')
    except SocketError as e:
        my_log.info(':::::::WARNING::::::: socket error number: ' + str(errno.errorcode[e.errno]) + ' for ' + pod + ' on hour ' + str(hr) + ' Try: ' + str(try_ctr) + ' Message: ' + os.strerror(e.errno))
        time.sleep(30)
        creds = None if creds_file is None else otp.sso(creds_file)
        br = sfm_on.InitiateProxySession('https://aloha.my.salesforce.com', useCookies=True, creds=creds)
    except IOError as e:
        my_log.info(':::::::WARNING::::::: broken pipe: ' + str(errno.errorcode[e.errno]) + ' for ' + pod + ' on hour ' + str(hr) + ' Try: ' + str(try_ctr) + ' Message: ' + os.strerror(e.errno))
        time.sleep(30)
        creds = None if creds_file is None else otp.sso(creds_file)
        br = sfm_on.InitiateProxySession('https://aloha.my.salesforce.com', useCookies=True, creds=creds)
    return


def splunk_date(yy, mm, dd, hr):
    if hr is None:
        my_log.info('invalid hour value')
        sys.exit(0)
    date = mm + '/' + dd + '/' + yy
    hr = str(hr) if hr > 9 else '0' + str(hr)
    day_str = date + ':' + hr + ':00:00'
    return day_str


def second_act(w_list, yy, mm, dd, f_out, dir_out):  # find what work if left to do
    redos = []
    for w in w_list:
        pod, hr = w
        f_path = get_outfile(f_out, dir_out, pod, yy, mm, dd, hr)
        if not os.path.isfile(f_path):
            redos.append((pod, hr))
        else:
            if os.path.getsize(f_path) == 0:
                redos.append((pod, hr))
    return redos


def parallel_exec(redo_pods, q_tail, kwargs_create, kwargs_results, kwargs_splunk, f_out, dir_out, yy, mm, dd, creds_file, str_mode, start_t, try_ctr):
    parallel_pod_apt = pl.simple_parallel(pod_apt, 5)
    # t_start = time.time()
    try:
        parallel_pod_apt(redo_pods, q_tail, kwargs_create, kwargs_results, kwargs_splunk, f_out, dir_out, yy, mm, dd, creds_file, str_mode, start_t, try_ctr)
    except SocketError as e:
        my_log.info(':::::::WARNING::::::: connection error number: ' + str(errno.errorcode[e.errno]) + ' for parallel exec. Message: ' + os.strerror(e.errno)) + ' Try: ' + str(try_ctr)
        time.sleep(30)
        creds = None if creds_file is None else otp.sso(creds_file)
        br = sfm_on.InitiateProxySession('https://aloha.my.salesforce.com', useCookies=True, creds=creds)
    except IOError as e:
        my_log.info(':::::::WARNING::::::: broken pipe: ' + str(errno.errorcode[e.errno]) + ' for parallel exec. Message: ' + os.strerror(e.errno)) + ' Try: ' + str(try_ctr)
        time.sleep(30)
        creds = None if creds_file is None else otp.sso(creds_file)
        br = sfm_on.InitiateProxySession('https://aloha.my.salesforce.com', useCookies=True, creds=creds)
    return


def splunk_search(f_cfg, d_cfg=None):
    if d_cfg is None:
        with open(f_cfg, 'r') as fp:
            d_cfg = json.load(fp)
    start_t = time.time()
    q_file = d_cfg['query_file']
    overwrite = d_cfg['overwrite']
    creds = d_cfg['creds']
    if q_file is not None:
        with open(q_file, 'r') as fp:
            q_list = fp.readlines()
        q_body = ''.join(q_list)
    else:
        q_body = None
    yy, mm, dd = d_cfg.get('year', None), d_cfg.get('month', None), d_cfg.get('day', None)
    start_hr, end_hr = d_cfg.get('start_hour', None), d_cfg.get('end_hour', None)
    stream_mode = d_cfg.get('stream_mode', False)
    if start_hr is None or yy is None or mm is None or dd is None:
        print 'missing config parameters'
        sys.exit(0)
    if end_hr is None:
        end_hr = start_hr + 1

    yy = str(yy)
    mm = str(mm) if mm > 9 else '0' + str(mm)
    dd = str(dd) if dd > 9 else '0' + str(dd)
    date = yy + '-' + mm + '-' + dd

    is_serial = d_cfg['serial']

    f_out = d_cfg['f_out']
    dir_out = d_cfg['dir_out']
    pod_list = d_cfg['pods']
    span = '| bucket _time span=' + d_cfg['span']
    log_lines = '(' + ' OR '.join(['`logRecordType(' + l + ')`' for l in d_cfg['log_lines']]) + ')'
    output = '| stats ' + ','.join([val + ' as ' + key for key, val in d_cfg['output'].iteritems()]) + ' '
    group_by = 'by ' + ','.join(d_cfg['group_by'])

    argv = ['query_str', '--verbose=0']   # verbose can only be a CL arg. Does not work as kwargs_results key. The query string get in place later
    flags = []
    flags.extend(FLAGS_TOOL)
    flags.extend(FLAGS_CREATE)
    flags.extend(FLAGS_RESULTS)
    opts = cmdline(argv, flags, usage='wrong args')

    kwargs_splunk = ut.dslice(opts.kwargs, ut.FLAGS_SPLUNK)
    kwargs_create = ut.dslice(opts.kwargs, FLAGS_CREATE)
    kwargs_results = ut.dslice(opts.kwargs, FLAGS_RESULTS)
    kwargs_results['output_time_format'] = '%Y-%m-%d %H:%M:%S'
    kwargs_results['output_mode'] = 'csv'
    kwargs_results['count'] = 50000
    kwargs_results['offset'] = 0

    if q_body is None:
        q_tail = log_lines + '\n\t' + span + '\n' + output + group_by
    else:
        q_tail = log_lines + '\n\t' + span + '\n' + q_body + '\n\t' + output + group_by       # part of the query
    work_list = [x for x in itertools.product(pod_list, range(start_hr, end_hr + 1))]     # work to do

    try_ctr = 0       # counts how many tries we have done
    work_left = []    # work left to do
    while try_ctr < MAX_TRIES:
        if try_ctr == 0:  # if overwrite is True, redo all the work at try = 0. For try > 0 do what is missing
            work_left = second_act(work_list, yy, mm, dd, f_out, dir_out) if overwrite is False else work_list
        else:
            work_left = second_act(work_list, yy, mm, dd, f_out, dir_out)
        try_ctr += 1
        my_log.info(f_cfg + ' for date ' + date + ': ' + str(len(work_left)) + ' jobs pending out of ' + str(len(work_list)) + '. Try number: ' + str(try_ctr))

        if len(work_left) > 0:
            if is_serial:
                _ = [pod_apt(w, q_tail, kwargs_create, kwargs_results, kwargs_splunk, f_out, dir_out, yy, mm, dd, creds, stream_mode, start_t, try_ctr) for w in work_left]
            else:
                parallel_exec(work_left, q_tail, kwargs_create, kwargs_results, kwargs_splunk, f_out, dir_out, yy, mm, dd, creds, stream_mode, start_t, try_ctr)
        else:
            break
        if time.time() - start_t > 86400.0:
            my_log.info(f_cfg + ' for date ' + date + ': ' + str(len(work_list) - len(work_left)) + ' out of ' +
                        str(len(work_list)) + ' jobs completed in ' + str(int(time.time() - start_t)) + ' secs and ' + str(try_ctr - 1) +
                        ' retries. \nExiting. @@@@@Process running for too long\n\n')
            sys.exit(0)

    # write final
    my_log.info(f_cfg + ' for date ' + date + ': ' + str(len(work_list) - len(work_left)) + ' out of ' +
                str(len(work_list)) + ' jobs completed in ' + str(int(time.time() - start_t)) + ' secs and ' + str(try_ctr - 1) + ' retries. DONE!\n\n')


if __name__ == '__main__':
    if len(sys.argv) != 2:
        my_log.info('invalid parameters' + str(sys.argv))
        sys.exit(0)
    splunk_search(sys.argv[1])
