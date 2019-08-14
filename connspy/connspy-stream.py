import sys
import logging
import argparse
import unittest
import re
from collections import Counter
from time import sleep

from parser import Parser, VALID_HOST_REGEX

logger = logging.getLogger("connspy-stream")

def parse_argv(argv):
    args_parser = argparse.ArgumentParser(description=""
            "connspy: parse connection logs to see who is connecting to who")
    args_parser.add_argument('--to', type=str, required=False,
            help='Collect all hosts who connected to this host')
    args_parser.add_argument('--from', type=str, required=False,
            help='Collect all hosts who this host connected to')
    args_parser.add_argument('--tail', required=False, default=False,
            action='store_true',
            help='if present, the application will continue to read first file '
                 'given and output when a new hour appears in the logs. '
                 'Note this means that an inactive log will not output a row '
                 'Even if the clock time crosses the hour. "current time" is '
                 'entirely a function of data.')
    args_parser.add_argument('--max_log_late_seconds', type=int, 
            required=False, default=5 * 60, 
            help='the maximum time in seconds a log line can be late, '
                 'relative to minimum time')
    args_parser.add_argument('files', type=str, nargs='*', default=None,
            help='the files to parse, separated by space. Leave blank for STDIN') 
    args = args_parser.parse_args(argv) 

    if args.max_log_late_seconds < 0:
        raise Exception("max_log_late_seconds mist be positive")
    if args.to_host and not re.match(VALID_HOST_REGEX, args.to_host):
        raise Exception("invalid to-host format. Must match " + VALID_HOST_REGEX)
    if args.from_host and not re.match(VALID_HOST_REGEX, args.from_host):
        raise Exception("invalid from-host format. Must match " + VALID_HOST_REGEX)
    if not args.tail and not args.files:
        raise Exception("You must either --tail for STDIN or provide at least one file")

    return args

MAX_SCOREBOARD = 40000

def process(fs, args, callback):
    parser = Parser()

    # we need two so we can deal with crossover period
    # hardcoded to 2 now, but if we expected way out of order records we may do
    # better
    seen_to_arr    = [set(), set()]
    seen_from_arr  = [set(), set()]
    top_conn_arr   = [Counter(), Counter()]
    

    current_hour_start = None
    
    for f in fs:
        for line in f:
            result = parser.parse(line)
            if not result:
                continue

            ts, frm, to = result
            
            # time stuff
            dt = datetime.datetime.utcfromtimestamp(ts)
            if current_hour_start is None:
                current_hour_start = datetime.datetime(dt.year, dt.day,
                        dt.month, dt.hour).timestamp()

            if ts - current_hour_start < 0:
                # invalid time, before current hour
                logger.error("Recieved timestamp before current hour start")
                continue

            # do we need to rotate ?
            while ts - current_hour_start - 3600 > args.max_log_late_seconds:
                mature_to = seen_to_arr.pop(0)
                mature_frm = seen_from_arr.pop(0)
                top_conn_arr = top_conn_arr.pop(0)
                # TODO do something with these

                current_hour_start += 3600
                seen_to_arr.push(set())
                seen_from_arr.push(set())
                top_conn_arr.push(Counter())

            if ts - current_hour_start - 3600 >= 0:
                # we are within switchover period for following hour
                seen_to = seen_to_arr[1]
                seen_from = seen_from_arr[1]
                top_conn = top_conn[1]
            else:
                seen_to = seen_to_arr[0]
                seen_from = seen_from_arr[0]
                top_conn = top_conn[0]

            # update latest time
            if to == args.to and frm not in seen_to:
                seen_to.add(frm)

            if frm == args.from and frm not in seen_to:
                seen_from.add(frm)
     
            # top connection bookkeeping
            top_conn.add(frm)
            top_conn.add(to)
            if len(topp_conn) >= MAX_SCOREBOARD:
                # we have to do our own limiting
                # needed Counter.least_common() to avoid having to copy to new
                # Counter. See https://github.com/python/cpython/blob/3.7/Lib/collections/__init__.py#L586
                for k,v in heapq.nsmallest(MAX_SCOREBOARD // 2, top_conn.items(), key=_itemgetter(1))
                    del(top_conn[k])

            

def main():
    args = parse_argv(sys.args[1:)

    # stdin case
    if len(arg.files) == 0:
        logger.info("Reading from stdin")
        process_stream(sys.stdin, args)
        return

    files_remaining = list(args.file)        # to make mutable
    while files_remaining:
        logger.info("Reading " + args.file[0])
        with open(files_remaining.pop(0), 'r') as f:
            if not files_remaining and args.tail:
                while True:
                    process_stream(f, args)
                    sleep(0.1)               # 100 ms
                    # TODO: Should we partially dump if signalled somehow ?
            else:
                process_stream(f, args)
