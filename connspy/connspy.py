import sys
import logging
import argparse
import unittest
import re

from collections import namedtuple 
from connspy.parser import Parser, VALID_HOST_REGEX
from connspy.binaryseek import seek_just_before_index

logger = logging.getLogger("connspy")

def parse_argv(argv):
    args_parser = argparse.ArgumentParser(description=""
            "connspy: parse connection logs to see who is connecting to who")
    args_parser.add_argument('--to', type=str, required=False,
            help='Collect all hosts who connected to this host')
    args_parser.add_argument('--time_init', type=str, required=True,
            help='the earliest time stamp of the log entries we should consider')
    args_parser.add_argument('--nofastseek', action='store_true',
            default=False, help='do not use fast block seek to start time')
    args_parser.add_argument('--time_end', type=str, required=True,
            help='the end of the time stamp range, noninclusive'),
    args_parser.add_argument('--max_log_late_seconds', type=int, 
            required=False, default=5 * 60, 
            help='the maximum time in seconds a log line can be late, relative to minimum time')
    args_parser.add_argument('file', type=str, default=None,
            help='the file to parse') 

    args = args_parser.parse_args(argv) 
    args.to = args.to.lower()

    if args.max_log_late_seconds < 0:
        raise Exception("max_log_late_seconds mist be positive")
    args.time_init = Parser.parse_ts(args.time_init)
    if not args.time_init:
        raise Exception("time_init invalid")
    args.time_end = Parser.parse_ts(args.time_end)
    if not args.time_end:
        raise Exception("time_end invalid")
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
        if not args.nofastseek:
            seek_just_before_index(args.file, f, args.time_init - args.max_log_late_seconds)
        process_stream(f, args, lambda x: print (x))

if __name__ == '__main__':
    logger.info("Called with: " + str(sys.argv))
    main()
