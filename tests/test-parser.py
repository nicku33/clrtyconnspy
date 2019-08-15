import unittest
from connspy.parser import Parser

class ParserTest(unittest.TestCase):
    def testLines(self):
        p = Parser()
        
        # expected to pass
        self.assertTupleEqual((1565293595.0, 'a', 'b'), p.parse("1565293595 a b"))
        self.assertTupleEqual((1565293593.0, 'a', 'b'), p.parse("1565293593 A b"))
        self.assertTupleEqual((1565293593.0, 'a', 'b'), p.parse("1565293593   A b  "))
        self.assertTupleEqual((1565293593.0, 'a', 'b'), p.parse("  1565293593 A b  "))
        self.assertTupleEqual((1565293593.0, '1', 'b'), p.parse("  1565293593 1 b  "))
        # explicit ms
        self.assertTupleEqual((1565293593.123, '1', 'b'), p.parse("  1565293593.123 1 b  "))
        # implicit ms
        self.assertTupleEqual((1565293593.123, '1', 'b'), p.parse("  1565293593123 1 b  "))

        # expected to fail
        self.assertIsNone(p.parse("")[0])
        self.assertIsNone(p.parse("          ")[0])
        self.assertIsNone(p.parse("a  b  c")[0])
        self.assertIsNone(p.parse("1b  c")[0])
        self.assertIsNone(p.parse("1b  c")[0])
        self.assertIsNone(p.parse("1565293593 b  c  d")[0])


