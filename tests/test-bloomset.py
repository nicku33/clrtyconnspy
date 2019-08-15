import unittest
from connspy.bloomset import BloomStringSet

class BloomsetTest(unittest.TestCase):

    def testBasicMembership(self):

        s = BloomStringSet()
        s.add("1")
        s.add("1")
        self.assertTrue("1" in s)
        self.assertFalse("2" in s)
        s.add("2")
        self.assertTrue("2" in s)

        res = sorted(list(s))
        self.assertListEqual(["1","2"], res)

