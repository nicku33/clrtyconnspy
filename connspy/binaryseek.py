import unittest
import tempfile
import os

import logging

logger = logging.getLogger(__name__)
block_size = 4096  # bytes. note typical ssd is 2-4 MB and 8kb pages

def seek_just_before_index(path, f, target):
    """
    This function accepts an open file handle object
    and attempts to seek through the file via
    binary search and return the file handle
    at a start of line before the 
    index desired, within 8kb by default
    This matches the page cache size on SSDs.

    path: path to file
    f: file object (already opened)
    target: numerical timetamp desired to be close to
    """

    if not f.readable():
        raise Exception(f"{path} is not readable")

    if not f.seekable():
        logger.info("Was asked to seek from a resource that is not seekable()")
        return # it's prob a stdio stream or something

    statinfo = os.stat(path)
    size = statinfo.st_size 
    last_block = size // block_size  # rounds down

    # less than 5 blocks not worth it
    if last_block < 5:
        return

    new_start = 0
    lo = 0
    hi = last_block
    
    iteration_limit = 20                  # for safety
    while (iteration_limit >= 0) and (hi - lo > 1):
        iteration_limit -= 1
        mid = (lo + hi) // 2
        f.seek(mid * block_size, os.SEEK_SET)  # from start of file
        f.readline()                      # blow this one, likely incomplete
        line = f.readline()
        index = int(line.split()[0])

        logger.info(f"new_start: {new_start}, hi: {hi}, lo: {lo}, {line}")

        # TODO: some more graceful error handling on that
        if index  < target:
            lo = mid
            new_start = f.tell()              # get current seek position
            continue
        hi = mid

    f.seek(new_start)
    # we are within 2 kb of the value we want, anyway


class TestBinarySeek(unittest.TestCase):
    def test_binary_seek(self):
        # let's make a temporary file
        path = None
        d = {}
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            path = f.name
            for ts in range(10000,20000):
                d[ts] = f.tell()
                s = f"{ts} " + "a" * 500 + " " + "b" * 500 + "\n"
                f.write(s)
            f.close()

        with open(path, 'r') as f:
                
            # if we want something before, it is ok, we should get beginning 
            # looking for an index earlier than the minimum should wield
            # a position of 0 in the file
            seek_just_before_index(path, f, 9500)
            self.assertEqual(0, f.tell())

            # for something in the middle we should be less than
            # 2048 bytes before actual start of the line 
            f.seek(0)
            seek_just_before_index(path, f, 12042)
            actual = d[12042]
            self.assertTrue(0 <= actual-f.tell() < 2 * block_size)
            # also, we should get a valid line
            line = f.readline().split()
            self.assertEqual(3, len(line))
            # and our index should be within 400 items
            self.assertTrue(11600 < int(line[0]) <= 12042)

            # try again with another one
            f.seek(0)
            seek_just_before_index(path, f, 17042)
            actual = d[17042]
            self.assertTrue(0 <= actual-f.tell() < 2 * block_size)
            # also, we should get a valid line
            line = f.readline().split()
            self.assertEqual(3, len(line))
            # and our index should be within 400 items
            self.assertTrue(16700 < int(line[0]) <= 17042)


            # finally, if we go past the end, we should return last index
            f.seek(0)
            seek_just_before_index(path, f, 19999)
            actual = d[19999]
            self.assertTrue(0 <= actual - f.tell() <= 2 * block_size )

        
