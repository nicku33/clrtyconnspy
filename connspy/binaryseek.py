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

    # we are within 2 blocks of the file location we want, anyway
    f.seek(new_start)
        
