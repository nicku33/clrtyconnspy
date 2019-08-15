import unittest
import datetime
import logging 
from tempfile import NamedTemporaryFile
from connspy.stream import process, parse_argv

# datetime.datetime(2019,3,1,14,0,0).utctimestamp() = 1551466800

HOUR1 = 1551466800
HOUR2 = 1551470400 

class StreamTest(unittest.TestCase):

    def testCrossingHour(self):
        with NamedTemporaryFile('w') as f1:
            with open(f1.name, 'r') as f2:
                args = parse_argv(f'--only_complete_hours --to b --from a {f1.name}'.split())

                res = []
                def callback(hr, to, frm, most):
                    res.append([hr, list(to), list(frm), most])

                f1.write(f"{HOUR1 + 3400} a b\n")
                f1.write(f"{HOUR1 + 3450} d b\n")
                f1.write(f"{HOUR1 + 3500} a c\n")
                f1.write(f"{HOUR1 + 3550} a c\n")
                
                # this is from the new hour
                f1.write(f"{HOUR1 + 3700} b a\n")

                # this is out of order from the older hour
                f1.write(f"{HOUR1 + 3570} a d\n")
                f1.flush()
                
                process([f2], args, callback)
                # we should see nothing
                self.assertEqual(0, len(res))
              
                res = []
                # this will sum everything
                f1.write(f"{HOUR1 + 4500} a e\n")
                f1.write(f"{HOUR1 + 4550} e d\n")
                f1.write(f"{HOUR1 + 4900} e b\n")
                f1.flush()

                f2.seek(0)
                process([f2], args, callback)

                self.assertEqual(1, len(res))

                self.assertEqual(HOUR1, res[0][0])  # current hour
                # to b
                self.assertListEqual(['a', 'd'], sorted(res[0][1]))
                # from a
                self.assertListEqual(['b', 'c', 'd'], sorted(res[0][2]))
                # most
                self.assertEqual('a', res[0][3])

                res = []
                f1.write(f"{HOUR1 + 4500 + 3600} a f\n")
                f1.flush()

                f2.seek(0)
                process([f2], args, callback)

                self.assertEqual(2, len(res))
                self.assertEqual(HOUR1 + 3600, res[1][0])  # current hour
                # to b
                self.assertListEqual(['e'], sorted(res[1][1]))
                # from a
                self.assertListEqual(['e'], sorted(res[1][2]))
                # most
                self.assertEqual('e', res[1][3])


    def testMultipleFiles(self):

        with NamedTemporaryFile('w', delete=False) as f1, NamedTemporaryFile('w', delete=False) as f2:
            f1.write(f"{HOUR1 + 3200} a b\n")  
            f1.write(f"{HOUR1 + 3800} c b\n") # h2
            f1.write(f"{HOUR1 + 3805} e b\n") # h2
            f1.write(f"{HOUR1 + 3400} a c\n")
            f1.write(f"{HOUR1 + 3550} c a\n")
            f2.write(f"{HOUR1 + 3550} a b\n")
            f2.write(f"{HOUR1 + 3552} b d\n")
            f2.write(f"{HOUR1 + 3700} d c\n") # h2
            f2.write(f"{HOUR1 + 4800} f b\n") # h2  
         
        with open(f1.name, 'r') as r1, open(f2.name, 'r') as r2:
            args = parse_argv(f'--to b --from a {f1.name} {f2.name}'.split())

            res = []
            def callback(hr, to, frm, most):
                res.append([hr, list(to), list(frm), most])

            process([r1, r2], args, callback)
        
            self.assertEqual(2, len(res))
            self.assertEqual(HOUR1, res[0][0])  # current hour
            self.assertListEqual(['a'], sorted(res[0][1]))
            self.assertListEqual(['b', 'c'], sorted(res[0][2]))
            self.assertEqual('a', res[0][3])

            self.assertEqual(HOUR2, res[1][0])  # current hour
            self.assertListEqual(['c', 'e', 'f'], sorted(res[1][1]))
            self.assertListEqual([], sorted(res[1][2]))
            self.assertEqual('b', res[1][3])

    def testTailing(self):
        # this is the tricky one, we can't really use the
        # callback method we used before because 
        # we want to feed a live process and AFAIK interprocess callbacks
        # with python aren't really possible out of the box

        # we can, however, just treat this as an integration test
        # and watch stdout for output, or direct output to a file

        # we'd also want to test things like
        # truncating the file (!!) 
        # writing a new file to the same path (diff inode)
    
        # I think I just need to test this manually
        pass

"""
SPY=connspy/connspy-stream.py 
DATA=/tmp/connspy_$RANDOM.txt
OUT =/tmp/connspy_$RANDOM.txt

# launch with output file not even there
# should fail
$SPY $DATA --to x --from y

$SPY --tail $DATA   
# process should remain active
echo "1576815811 a b" > $DATA
echo "1576815811 a c" > $DATA
echo "1576815811 a c" > $DATA

# now go past end point
echo "1576815811 a c" > $DATA

# we expect some output now
diff $EXPECTED $DATA

# let's even remove the $DATA file
rm -f $DATA

# add some more
echo "1576815811 a c" > $DATA
echo "1576815811 a c" > $DATA

# then cross the line
# we should have 2 lines in output
diff EXPECTED2 $DATA

# test 2
# test multiple file
$SPY sample_data/t1-1.txt sample_data/t1-2.txt sample_data/t1-3.txt > $OUT
diff $OUT $EXPECTED1

"""



