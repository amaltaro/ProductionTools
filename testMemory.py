"""
Requirements to run this script:
 * condor_schedd daemon

Jobs are actually submitted to the local condor queue, but they won't
run because the x509 ads were not properly set (see REPLACE-ME)
"""
from __future__ import print_function

import os
import sys
import time
import random
import string
import psutil
from memory_profiler import profile


def randomString(stringLength):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


class MSBlock():
    __slots__ = ["name", "blockSize", "locations"]

    def __init__(self, name, size, locations):
        self.name = name
        self.size = size
        self.location = locations


profileFp = open('slots.log', 'w+')
@profile(stream=profileFp)
def createSlots(numDsets):
    """Create all the necessary objects and submit to condor"""
    data = {}
    for i in range(numDsets):
        dataset = randomString(100)
        data.setdefault(dataset, [])
        # 100 blocks
        for b in range(100):
            blockName = randomString(125)
            locations = []
            for site in range(3):
                locations.append(randomString(20))
            block = MSBlock(blockName, random.randint(1e5, 1e7), locations)
            data[dataset].append(block)
    print("%s dataset objects created" % numDsets)


profileFp = open('dicts.log', 'w+')
@profile(stream=profileFp)
def createDicts(numDsets):
    """Create all the necessary objects and submit to condor"""
    data = {}
    for i in range(numDsets):
        dataset = randomString(100)
        data.setdefault(dataset, {})
        # 100 blocks
        for b in range(100):
            blockName = randomString(125)
            block = {blockName: {"blockSize": random.randint(1e5, 1e7), "locations": []}}
            for site in range(3):
                block[blockName]["locations"].append(randomString(20))
            data[dataset].update(block)
    print("%s dataset dicts created" % numDsets)



def main():
    # make it a different function such that gc can collect objects
    thisProcess = psutil.Process(os.getpid())
    print("Initial dict RSS: %s" % thisProcess.memory_info().rss)
    createDicts(1000)
    print("Final dict RSS: %s" % thisProcess.memory_info().rss)
    print("Initial slots RSS: %s" % thisProcess.memory_info().rss)
    createSlots(1000)
    print("Final slots RSS: %s" % thisProcess.memory_info().rss)


if __name__ == "__main__":
    sys.exit(main())
