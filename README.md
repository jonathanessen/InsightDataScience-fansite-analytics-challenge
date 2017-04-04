Insight Data Engineering Challenge
======

 * **Feature 1**:
 
 I used collections.Counter to keep track of the number of times each host made an HTTP request. Since Counters are hashable, this should be an efficient approach.


 * **Feature 2**:
 
 I used collections.Counter again to keep track of the number of bytes sent due to each resource. I made sure to strip off any additional queries in the HTTP request using urlparse.
 
 
 * **Feature 3**:
 
 I step through the log file and make a list of the epoch times of each request, then I form the cumulative distribution function and use finite differencing to get the number of requests in each hour-long interval.

 * **Feature 4**:
 
 I maintain dictionaries of 'bad hosts' -- hosts that have generated an error (code 3xx or 4xx) in the past 20s, as well as 'blocked hosts' -- hosts that have been blocked for 5 minutes. The keys are the hostnames, and the values are the error time (bad host) and the unblocking time (blocked host). If a host on the block list sends a request, this is logged.
