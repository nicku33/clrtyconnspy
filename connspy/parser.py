import logging
import re
import datetime

VALID_HOST_REGEX = r"[0-9a-z]+"
INVALID          = (None, None, None)
logger = logging.getLogger("parser")

class Parser():
    """
    This Parser currently rigidly accepts lines
    as whitespace delimited TS, FROM, TO and
    enforces some validity of inputs.
    """

    def __init__(self):
        self.valid_domain_regex = re.compile(VALID_HOST_REGEX)

    def parse(self, line):
        # nb. no argument to .split means split on multiple whitespace '\s+'
        # also ignores initial and final whitespace

        ele = line.lower().split()

        if len(ele) != 3:
            logger.error("Invalid line, too many elements: " + line)
            return INVALID 

        try:
            ts = float(ele[0])
            if ts > 999999999999:
                ts /= 1000.0
            dt = datetime.datetime.fromtimestamp(ts)
            ts = dt.timestamp()
        except:
            logger.error(f"Cannot parse {ele[0]} into a timestamp")
            return INVALID

        # regex useful if you need character range guaratees for trie or compression)
        if not (self.valid_domain_regex.match(ele[0]) and
                self.valid_domain_regex.match(ele[1])):
            logger.error("Invalid domains for line: " + line)
            return INVALID
       
        return ts, ele[1], ele[2]

