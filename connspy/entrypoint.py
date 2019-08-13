import sys
import logging
import argparse
import unittest
import re

from collections import namedtuple 
from .parser import Parser, VALID_HOST_REGEX, MIN_TS

logger = logging.getLogger("parser")

def process_stream(f, args, callback):

    parser = Parser()
    to_found = set()     
    latest_ts = None

    for line in f:
        result = parser.parse(line)

        if not result:
            # invalid line, perhaps logging should be here rather
            # than parser class, but we have more context there...
            continue
 
        ts, frm, to = result

        if not latest_ts or ts > latest_ts:
            latest_ts = ts

        if args.time_end and latest_ts >= (args.time_end + args.max_log_late_seconds):
            # we can return early
            return

        if args.to_host and to == args.to_host and frm not in to_found and args.time_init <= ts < args.time_end:
            to_found.add(frm)
            # output as we go, in case user sees something that makes them
            # cancel
            callback(frm)

def parse_argv(argv):

    args_parser = argparse.ArgumentParser(description=""
    "connspy: parse connection logs to see who is connecting to who")

    args_parser.add_argument('--to_host', type=str, required=False,
    help='Collect all hosts who connected to this host')

    args_parser.add_argument('--from_host', type=str, required=False,
            help='Collect all hosts who this host connected to')

    args_parser.add_argument('--busiest_from_host', required=False,
            action='store_true',
            help='Output the host who initiated the most connections')

    args_parser.add_argument('--hourly', required=False, action='store_true',
            help='Group output by hour')

    args_parser.add_argument('--time_init', type=int, required=True,
            help='the earliest time stamp of the log entries we should consider')

    args_parser.add_argument('--time_end', type=int, required=True,
            help='the end of the time stamp range, noninclusive'),
    
    args_parser.add_argument('--tail', required=False, default=False,
            action='store_true',
            help='if present, the application will continue to read first file'
                 'given and output when a new hour GMT is complete, as specified by'
                 'max_log_late_seconds')

    args_parser.add_argument('--max_log_late_seconds', type=int, required=False, default=5 * 60, help='the maximum time in seconds a log line can be late, relative to minimum time')
            
    args_parser.add_argument('files', type=str, nargs='*', default=None,
            help='the files to parse, separated by space. Leave blank for STDIN') 

    args = args_parser.parse_args(argv) 

    # some vailidity checks
    if args.time_init < MIN_TS:
        raise Exception("time_init is too small")
    if args.time_end < MIN_TS:
        raise Exception("time_end is too small")
    if args.time_end < args.time_init:
        raise Exception("time_end is before time_init")
    if args.max_log_late_seconds < 0:
        raise Exception("max_log_late_seconds mist be positive")
    if args.to_host and not re.match(VALID_HOST_REGEX, args.to_host):
        raise Exception("invalid to-host format. Must match " + VALID_HOST_REGEX)
    if args.from_host and not re.match(VALID_HOST_REGEX, args.from_host):
        raise Exception("invalid from-host format. Must match " + VALID_HOST_REGEX)

    return args

def main_scanner(args):

    f = None
    try:
        if not args.file:
            f = sys.stdin
        else:
            logger.info("opening " + args.file)
            f = open(args.file, 'r')
        process_stream(f, args, lambda x: print (x))

    finally:
        # can't use 'with' syntax bc of stdin option
        if f:
            f.close()

# must keep in sync w parseargs
class ProcessorTest(unittest.TestCase):

    def testSimpleExample(self):

        # simple exmaple from docs
        f = [ "1576815793 quark garak", 
              "1576815795 brunt quark", 
              "1576815811 lilac garak"]
        args = parse_argv('--time_init 1567000000 --time_end 1580000000 --to_host garak'.split())
        out = []
        process_stream(f, args, lambda x: out.append(x))
        out.sort()
        self.assertListEqual(['lilac', 'quark'], out)
        
    def testDuplicatesRemoved(self):
        # ts, frm, to
        f = [ "1570000000 b a", 
              "1570000001 x y", 
              "1570000002 a y", 
              "1570000003 a x", 
              "1570000004 b y",
              "1570000005 x y",
              "1570000006 b x"]
        out = []
        args = parse_argv('--time_init 1567000000 --time_end 1580000000 --to_host x'.split())
        process_stream(f, args, lambda x:
                out.append(x))
        out.sort()
        self.assertListEqual(['a', 'b'], out)

        out = []
        args = parse_argv('--time_init 1567000000 --time_end 1580000000 --to_host y'.split())
        process_stream(f, args, lambda x: out.append(x))
        out.sort()
        self.assertListEqual(['a', 'b', 'x'], out)

    def testTimeFilterWorks(self):
        # ts, frm, to
        f = [ "1570000000 b a", 
              "1570000001 x y", 
              "1570000002 a y",  # STARTS 
              "1570000003 a x", 
              "1570000004 b y",  # ENDS
              "1570000005 x y",  # this should be missed
              "1570000006 b x"]
        out = []
        
        args = parse_argv('--time_init 1570000002 --time_end 1570000005 '
                          '--to_host y'.split())
        process_stream(f, args, lambda x: out.append(x))
        out.sort()
        self.assertListEqual(['a', 'b'], out)

    def testOutOfOrderAccomodated(self):
        # ts, frm, to
        f = [ "1570000000 b a", 
              "1570000001 x y", 
              "1570000002 a y",  # STARTS 
              "1570000003 a x", 
              "1570000005 x y",  # this should be missed due to cutoff
              "1570000008 c y",  # this provides the latest timestamp
              "1570000004 d y",  # still valid 
              "1570000009 x y",  # this should make the end timestamp kick in 
              "1570000004 e y"]  # thiis is outside cutoff time now 
              
        out = []
        args = parse_argv('--time_init 1570000002 --time_end 1570000005 '
                          '--to_host y --max_log_late_seconds 4'.split())
        
        process_stream(f, args, lambda x: out.append(x))

        out.sort()
        self.assertListEqual(['a', 'd'], out)


if __name__ == '__main__':
    logger.info("Called with: " + str(sys.argv[1:]))
    main_scanner(parse_argv(sys.argv[1:]))
