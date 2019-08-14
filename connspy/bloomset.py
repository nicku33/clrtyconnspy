from bloom_filter import BloomFilter
from tempfile import SpooledTemporaryFile
import unittest

class BloomStringSet:
    """
    BloomSet implements set membership in a fixed memory footprint,
    but can reproduce the keys by streaming them to temporary files.
    As it's based on a bloom filter, there is a risk of false positives,
    which would cause some missing keyue, however the likelihood
    of such is controllable through the parameters. Temporary files
    are only created after they reach 5MB, otherwise stay in memory.
    
    cardinality: the estimated maximum number of unique elements.
                 As one goes beyond this number, risk of collision
                 increases, but sloooooowly.

    error_rate:  the false positive rate you are comfortable with when
                 the cardinality number is reached.

    """

    def __init__(self, cardinality=10 ** 6, error_rate=10 ** -9):
        self.bloom = BloomFilter(cardinality, error_rate)
        self.file  = SpooledTemporaryFile(max_size=(2 ** 20) * 5, mode='w')
        self.closed = False
        
    def add(self, key):
        if self.closed:
            raise Exception("Cannot add new element after attempting to read")

        if type(key) is not str:
            raise Exception("Can only use string keys for now")

        if key in self.bloom:
            return False

        self.bloom.add(key)
        self.file.write(key + "\n")

    def __contains__(self, key):
        return key in self.bloom

    def items(self):
        # TODO: if items() not totally consumed
        # file pointer won't be at end, so shouldn't append
        # will just block this for now
        self.closed = True

        self.file.seek(0)

        # must be a generator to avoid buffering list in memory
        for ele in self.file:
            ele = ele.strip()
            yield ele
