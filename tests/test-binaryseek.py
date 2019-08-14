import unittest
import tempfile
from connspy.binaryseek import seek_just_before_index, block_size

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


