import sys
import logging
import argparse
import re
import datetime
import time
from collections import Counter, defaultdict
from time import sleep

from connspy.bloomset import BloomStringSet
from connspy.parser import Parser, VALID_HOST_REGEX

logger = logging.getLogger("stream")
logger.setLevel(logging.DEBUG)


class LimitedCounter(Counter):
    def __init__(self, max_size=30000, reduce_factor=0.5):
        self.max_size = max_size
        self.reduce_factor = reduce_factor
        super().__init__(self)

    def add(self, key):
        super().add(key)
        # once in a while, maintain the size in place
        if len(self.items()) >= self.max_size:
            desired_size = int(len(self.items()) * self.reduce_factor)
            # https://github.com/python/cpython/blob/3.7/Lib/collections/__init__.py#L586
            for k,v in heapq.nsmallest(desired_size, self.items(), key=_itemgetter(1)):
                del(self[k])

def parse_argv(argv):
    args_parser = argparse.ArgumentParser(description=""
            "connspy: parse connection logs to see who is connecting to who")
    args_parser.add_argument('--to', type=str, required=True,
            help='Collect all hosts who connected to this host')
    args_parser.add_argument('--from', type=str, dest='frm', required=True,
            help='Collect all hosts who this host connected to')
    args_parser.add_argument('--only_complete_hours', default=False,
            action='store_true', 
            help='Normally at end of batch, partially completed hours are '
                 'dumped. However you many only want completed hours')
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

    args.to = args.to.lower()
    args.frm = args.frm.lower()

    if args.max_log_late_seconds < 0:
        raise Exception("max_log_late_seconds mist be positive")
    if args.to and not re.match(VALID_HOST_REGEX, args.to):
        raise Exception("invalid to-host format. Must match " + VALID_HOST_REGEX)
    if args.frm and not re.match(VALID_HOST_REGEX, args.frm):
        raise Exception("invalid from-host format. Must match " + VALID_HOST_REGEX)
    if not args.tail and not args.files:
        raise Exception("You must either --tail for STDIN or provide at least one file")

    return args

# for convinience, but I should switch to keys
TO, FROM, MOST = 0, 1, 2
class Processor:

    def __init__(self, args, callback):
        self.args = args
        self.callback = callback
        self.hourly_summaries = defaultdict(lambda: [BloomStringSet(), BloomStringSet(), LimitedCounter()])

    def hour_of(self, ts):
        dt = datetime.datetime.utcfromtimestamp(ts)
        # while you could ts % 3600, there have been like 40 leap
        # seconds since 1970, and I'm sure UNIX type people
        # def care about that kind of stuff
        ts = datetime.datetime(dt.year, dt.month, dt.day, 
                                 dt.hour, 0, 0, 
                                 tzinfo=datetime.timezone.utc).timestamp()
        return ts

    def process(self, f, tail=False):
        parser = Parser()
        hourly_summaries = self.hourly_summaries
        args = self.args
        callback = self.callback

        # bookkeeping... we need current hour and
        # one for the upcoming hour

        while True:
            line = f.readline()
            if line == "":
                if tail:
                    time.sleep(0.5)
                    continue
                else:
                    break

            ts, frm, to = parser.parse(line)
            if not ts:
                continue
              
            # our start time is defines as the hour of the first time stamp
            ts_hr = self.hour_of(ts)
            summary = hourly_summaries[ts_hr]

            # seen hosts bookkeeping
            if to == args.to and frm not in summary[TO]:
                summary[TO].add(frm)

            if frm == args.frm and to not in summary[FROM]:
                summary[FROM].add(to)
     
            # top connection bookkeeping
            summary[MOST][frm] += 1
            summary[MOST][to] += 1

            # ok, if this timestamp is > than our waiting period
            # we can call the oldest hour summary mature

            current_hour = min(hourly_summaries.keys())

            if ts-current_hour > (3600 + args.max_log_late_seconds):
                summary = hourly_summaries[current_hour]
                callback(current_hour,
                    summary[TO],
                    summary[FROM],
                    summary[MOST].most_common(1)[0][0])
                del(hourly_summaries[current_hour])

    def dump_remaining(self): 
        hours = sorted(self.hourly_summaries.keys())
        for hour in hours:
            summary = self.hourly_summaries[hour]
            self.callback(hour,
                summary[TO],
                summary[FROM],
                summary[MOST].most_common(1)[0][0])


def output(hour, to, frm, most):
    for t in to:
        print (f"{hour}\tTO\t{t}")
    for f in frm:
        print (f"{hour}\tFROM\t{f}")
    print (f"{hour}\tMOST\t{most}")
    
def main():
    args = parse_argv(sys.argv[1:])
    logger.info(args)

    pr = Processor(args, output)

    # stdin case
    if len(args.files) == 0:
        logger.info("Reading from stdin")
        # not readline from stdin automatically blocks
        pr.process(sys.stdin)
        return

    files_remaining = list(args.files)        # to make mutable
    while files_remaining:
        logger.info("Reading " + files_remaining[0])

        # TODO: Error recovery on bad file ? better to fail or skip
        with open(files_remaining.pop(0), 'r') as f:
            should_tail = not files_remaining and args.tail
            pr.process(f, should_tail)

    if not args.only_complete_hours:
        pr.dump_remaining()


if __name__ == '__main__':
    logger.info("Called with: " + str(sys.argv))
    main()
