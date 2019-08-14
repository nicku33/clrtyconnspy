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
                args = parse_argv(f'--from a --to b {f1.name}'.split())

                f1.write("1551466800 a b\n")
                f1.write("1551466804 d b\n")
                f1.write("1551466809 a c\n")
                f1.write("1551466810 a c\n")
                # 1551470400
                f1.write("1551471401 c b\n")
                f1.write("1551471403 a d\n")
                f1.flush()
                
                fs = [f2]
                res = []
                callback = lambda hr, to, frm, most: res.append([hr, list(to),
                    list(frm), most])

                process(fs, args, callback)
                self.assertEqual(1, len(res))
                self.assertEqual(1551466800, res[0][0])
                self.assertListEqual(['a', 'd'], sorted(res[0][1]))
                self.assertListEqual(['b', 'c'], sorted(res[0][2]))
                self.assertEqual('a', res[0][3])

    def testMultipleFiles(self):
        pass

    def testTailing(self):
        pass

"""
    P   

#!/bin/bas
set -ex

echo "Running unit tests on connspy"
PYTHONPATH=connspy pytest tests/*.py

exit 0
echo "Running integration tests on connspy-stream"

# TODO: Rewrite this into a python integration test

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



