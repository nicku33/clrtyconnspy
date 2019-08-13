import unittest
import logging
import re

MIN_TS = 1565293593   # 2010 01 01
VALID_HOST_REGEX = r"[0-9a-z]+"
logger = logging.getLogger("parser")

class Parser():
    """
    This class takes care of all single line logging,
    enforcing validity of inputs.
    """

    def __init__(self):
        self.valid_domain_regex = re.compile(VALID_HOST_REGEX)

    def parse(self, line):

        line = line.lower()

        # no argument to .split means split on multiple whitespace '\s+'
        # also ignores initial and final whitespace

        line_array = line.split()
        if len(line_array) != 3:
            logger.error("Invalid line, too many elements: " + line)
            return None
        if line_array[0].isdigit():
            ts = int(line_array[0])
        else:
            logger.error("Invalid line, first param not timestamp: " + line)
            return None
        if ts < MIN_TS:
            logger.error("Invalid timestamp before 2010")
            return None

        frm = line_array[1]
        to = line_array[2]

        if not (self.valid_domain_regex.match(frm) and
                self.valid_domain_regex.match(to)):
            logger.error("Invalid domains for line: " + line)
            return None
        return (ts, frm, to) 


class ParserTest(unittest.TestCase):
    def testLines(self):
        p = Parser()
        
        # expected to pass
        self.assertTupleEqual((1565293593, 'a', 'b'), p.parse("1565293593 a b"))
        self.assertTupleEqual((1565293593, 'a', 'b'), p.parse("1565293593 A b"))
        self.assertTupleEqual((1565293593, 'a', 'b'), p.parse("1565293593   A b  "))
        self.assertTupleEqual((1565293593, 'a', 'b'), p.parse("  1565293593 A b  "))
        self.assertTupleEqual((1565293593, '1', 'b'), p.parse("  1565293593 1 b  "))

        # expected to fail
        self.assertIsNone(p.parse(""))
        self.assertIsNone(p.parse("          "))
        self.assertIsNone(p.parse("a  b  c"))
        self.assertIsNone(p.parse("1b  c"))
        self.assertIsNone(p.parse("1b  c"))
        self.assertIsNone(p.parse("1565293593.23 b  c"))
        self.assertIsNone(p.parse("1565293593 b  c  d"))

