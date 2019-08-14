import unittest
import logging
import re

VALID_HOST_REGEX = r"[0-9a-z]+"
INVALID          = (None, None, None)
logger = logging.getLogger("parser")

class Parser():
    """
    This Parser currently rigidly accepts lines
    as whitespace delimited TS,FROM,TO and
    enforcing validity of inputs.
    """

    def __init__(self):
        self.valid_domain_regex = re.compile(VALID_HOST_REGEX)

    def parse(self, line):
        # nb. no argument to .split means split on multiple whitespace '\s+'
        # also ignores initial and final whitespace

        line_array = line.lower().split()
        if len(line_array) != 3:
            logger.error("Invalid line, too many elements: " + line)
            return INVALID 
        if not line_array[0].isdigit():
            logger.error("Invalid line, first param not timestamp: " + line)
            return INVALID

        # regex useful if you need character range guaratees for trie or compression)
        if not (self.valid_domain_regex.match(frm) and
                self.valid_domain_regex.match(to)):
            logger.error("Invalid domains for line: " + line)
            return INVALID
       
        return int(line_array[0]), line_array[1], line_array[2]


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

