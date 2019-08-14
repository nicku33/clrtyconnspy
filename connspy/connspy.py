import sys
import logging
import argparse
import unittest
import re

from collections import namedtuple 
from parser import Parser, VALID_HOST_REGEX
from binaryseek import seek_just_before_index

logger = logging.getLogger("connspy")

def parse_argv(argv):
    args_parser = argparse.ArgumentParser(description=""
            "connspy: parse connection logs to see who is connecting to who")
    args_parser.add_argument('--to', type=str, required=False,
            help='Collect all hosts who connected to this host')
    args_parser.add_argument('--time_init', type=int, required=True,
            help='the earliest time stamp of the log entries we should consider')
    args_parser.add_argument('--time_end', type=int, required=True,
            help='the end of the time stamp range, noninclusive'),
    args_parser.add_argument('--max_log_late_seconds', type=int, 
            required=False, default=5 * 60, 
            help='the maximum time in seconds a log line can be late, relative to minimum time')
    args_parser.add_argument('file', type=str, nargs='*', default=None,
            help='the file to parse') 

    args = args_parser.parse_args(argv) 
    args.to = args.to.lower()

    MIN_TS = 1199145600   # Jan 1, 2008 midnight
    if args.max_log_late_seconds < 0:
        raise Exception("max_log_late_seconds mist be positive")
    if args.time_init < MIN_TS:
        raise Exception("time_init is too small")
    if args.time_end < MIN_TS:
        raise Exception("time_end is too small")
    if args.time_end < args.time_init:
        raise Exception("time_end is before time_init")
    if not re.match(VALID_HOST_REGEX, args.to):
        raise Exception(f"invalid to-host {args.to} . Must match " + VALID_HOST_REGEX)

    return args

# factored out for testing ease
def process_stream(f, args, callback):
    latest_ts = -float('inf')
    parser = Parser()
    seen   = set()

    for line in f:
        ts, frm, to = parser.parse(line)
        if not ts:       # invalid
            continue  
        latest_ts = max(ts, latest_ts)

        # can we stop early, including the out of order buffer ?
        if latest_ts >= (args.time_end + args.max_log_late_seconds):
            return

        if (args.time_init <= ts < args.time_end and
                        to ==      args.to  and 
                       frm not in  seen):
            seen.add(frm)
            callback(frm)


def main():
    args = parse_argv(sys.argv[1:])
    logger.info("opening " + args.file)

    with open(args.file, 'r') as f:
        seek_just_before_index(args.file, f, args.time_init)
        process_stream(f, args, lambda x: print (x))

# '''uncomment if not using via setuptools or testisp'''
# if __name__ == '__main__':
#    logger.info("Called with: " + str(sys.argv))
#    main_scanner(sys.argv)
