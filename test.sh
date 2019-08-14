#!/bin/bash
set -x

echo "Running unit tests on connspy"
export PYTHONPATH=connspy
pytest connspy/connspy.py connspy/parser.py connspy/binaryseek.py

exit 0
echo "Running integration tests on connspy-stream"

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




