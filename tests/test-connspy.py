import unittest
from connspy.connspy import process_stream, parse_argv

class ProcessorTest(unittest.TestCase):

    def testSimpleExample(self):
        f = [ "1576815793 quark garak", 
              "1576815795 brunt quark", 
              "1576815811 lilac garak"]
        args = parse_argv('--time_init 1567000000 --time_end 1580000000 --to garak dummyfile'.split())
        out = []
        process_stream(f, args, lambda x: out.append(x))
        self.assertListEqual(['lilac', 'quark'], sorted(out))
        
    def testDuplicatesRemoved(self):
        f = [ "1570000000 b a", 
              "1570000001 x y", 
              "1570000002 a y", 
              "1570000003 a x", 
              "1570000004 b y",
              "1570000005 x y",
              "1570000006 b x"]

        out = []
        args = parse_argv('--time_init 1567000000 --time_end 1580000000 --to x dummyfile'.split())
        process_stream(f, args, lambda x: out.append(x))
        self.assertListEqual(['a', 'b'], sorted(out))

        out = []
        args = parse_argv('--time_init 1567000000 --time_end 1580000000 --to y dummyfile'.split())
        process_stream(f, args, lambda x: out.append(x))
        self.assertListEqual(['a', 'b', 'x'], sorted(out))

    def testTimeFilterWorks(self):
        f = [ "1570000000 b a", 
              "1570000001 x y", 
              "1570000002 a y",  # STARTS 
              "1570000003 a x", 
              "1570000004 b y",  # ENDS
              "1570000005 x y",  # this should be missed
              "1570000006 b x"]
        out = []
        
        args = parse_argv('--time_init 1570000002 --time_end 1570000005 '
                          '--to y dummyfile'.split())
        process_stream(f, args, lambda x: out.append(x))
        out.sort()
        self.assertListEqual(['a', 'b'], sorted(out))

    def testOutOfOrderAccomodated(self):
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
                          '--to y --max_log_late_seconds 4 dummyfile'.split())
        
        process_stream(f, args, lambda x: out.append(x))

        out.sort()
        self.assertListEqual(['a', 'd'], sorted(out))
