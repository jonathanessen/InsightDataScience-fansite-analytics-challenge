## My basic approach is to step through the log file and store the necessary
## information in RAM. I suppose that for sufficiently large log files (many, many GB),
## we will eventually run out of RAM. In that case, we could store these
## variables on disk with pandas.HDFStore.

from collections import Counter
from urlparse import urlparse
import time
import numpy as np

log_file_name = './log_input/log.txt'

hosts_file_name = './log_output/hosts.txt'
host_counter = Counter()
top_hosts_num = 10

resource_file_name = './log_output/resources.txt'
resource_counter = Counter()
top_resource_num = 10

hours_file_name = './log_output/hours.txt'
requests = list()
request_window = 3600  # seconds
top_hours_num = 10

blocked_file_name = './log_output/blocked.txt'
bad_hosts = dict()
blocked_hosts = dict()
bad_window  = 20       # seconds
block_window = 5 * 60  # seconds
bad_codes = ['3', '4']
max_bad_requests = 3

## Ignoring timezone information and tuning for GMT-0700
##  -- not sure how to nicely deal with timezone in python 2
##  -- I tried dateutil.parser.parse() but it was very slow
##  -- better probably to use datetime in python 3
time_pattern = '%d/%b/%Y:%H:%M:%S'
time_offset = 7 * 3600 # sec

## Open server log file for reading, and blocked requests file (Feature 4) for writing
with open(log_file_name, 'r') as f_log, open(blocked_file_name, 'w') as f_blocked:
    for line in f_log:
        ## Parse the current line of the log file:
        ##   the name of the connecting host
        host_name = line.split(' - - ')[0]
        ##   the response code from the server
        response_code = line.split(' ')[-2].strip()
        ##   the website resource (stripping off query strings)
        resource = urlparse((''.join(line.split('"')[1])).split(' ')[1]).path
        ##   the time stamp
        time_stamp = line.split('[')[1].split(' ')[0]
        ##   the time zone
        time_zone  = line.split('[')[1].split(' ')[1][:-1]
        ##   the number of bytes sent
        try: bytes_sent = int(line.split(' ')[-1].strip())
        except: bytes_sent = 0        
        
        ## Convert time stamp to offset-epoch time 
        time_epoch = int(time.mktime(time.strptime(time_stamp, time_pattern))) - time_offset
        
        ## Store request-time and update counters
        requests.append(time_epoch)
        host_counter.update([host_name])
        resource_counter.update({resource: bytes_sent})

        ## Check if hostname is blocked
        if host_name in blocked_hosts:
            ## unblock if sufficient time has passed
            if time_epoch > blocked_hosts[host_name]:
                del blocked_hosts[host_name]
            ## otherwise log blocked request
            else:
                f_blocked.write(line)
                
        ## Maintain list of bad hosts (errors within 20s before now)
        if ((response_code[0] in bad_codes) and (host_name not in blocked_hosts)):
            ## add current host to 'bad list'
            if host_name in bad_hosts:
                bad_hosts[host_name].append(time_epoch)
            else:
                bad_hosts.update({host_name: [time_epoch]})
                
            ## prune 'bad list' of sufficiently-old access times
            while True:
                if bad_hosts[host_name][-1] - bad_hosts[host_name][0] > bad_window:
                    del bad_hosts[host_name][0]
                else:
                    break        
            
            ## maintain dict of blocked hosts vs unblocking time
            if len(bad_hosts[host_name]) >= max_bad_requests:
                blocked_hosts.update({host_name: time_epoch + block_window})

## log output for Feature 1
with open(hosts_file_name, 'w') as f:
    top_hosts = host_counter.most_common(top_hosts_num)
    for host in top_hosts:
        f.write(host[0] + ',' + str(host[1]) + '\n')

## log output for Feature 2
with open(resource_file_name, 'w') as f:
    top_resources = resource_counter.most_common(top_resource_num)
    for resource in top_resources:
        f.write(resource[0] + '\n')

## log output for Feature 3
with open(hours_file_name, 'w') as f:
    ## compute cumulative distribution function
    _time = np.arange(requests[0], requests[-1] + request_window)
    _requests = np.zeros_like(_time)
    _idx = np.array(requests) - requests[0] + 1
    np.add.at(_requests, _idx, 1)
    accum_requests = np.cumsum(_requests)
    
    ## use finite differences to get number of requests over hourly intervals
    diff_requests = accum_requests[request_window:] - accum_requests[:-request_window]

    ## use a counter to get busiest hours
    request_counter = Counter(dict(zip(_time[:-request_window].tolist(), diff_requests.tolist())))
    top_request_hours = request_counter.most_common(top_hours_num)

    for req_hr in top_request_hours:
        f.write(time.strftime(time_pattern, time.gmtime(req_hr[0]))
                + ' ' + time_zone + ',' + str(req_hr[1]) + '\n')           

