#!/usr/bin/env python
"""
Ssript to parse two json files:
 * one with parent dataset level run - lumi - fileid information
 * the other with child dataset (or block) level run - lumi - fileid information
and using a tuple of lumi and run numbers, find the parent file ids.
"""
from __future__ import print_function, division

import json
import os
import sys
import time

import psutil


def loadFiles():
    """Load the parent and children json files"""
    tStart = time.time()
    with open("parents_dbs.json") as jobj:
        parents = json.load(jobj)
    with open("children_dbs.json") as jobj:
        children = json.load(jobj)
    print("Time loading input json files: {}".format(time.time() - tStart))
    return parents, children


def convertToFrozenDict(parents):
    """
    Converts the list of tuples into a dictionary key'ed by a frozen set
    of run/lumi. It's value is actually the fileid
    """
    tStart = time.time()
    frozenDict = dict()
    for item in parents:
        frozenKey = frozenset(item[:2])
        frozenDict[frozenKey] = item[2]
    print("Time sorting parent/children data: {}".format(time.time() - tStart))
    return frozenDict


def main():
    # make it a different function such that gc can collect objects
    tStart = time.time()
    thisProcess = psutil.Process(os.getpid())
    print("Initial RSS memory footprint: {} kB".format(thisProcess.memory_info().rss / 1024))
    parents, children = loadFiles()
    print("Found {} parent entries".format(len(parents)))
    print("Found {} children entries".format(len(children)))

    parents = convertToFrozenDict(parents)
    print("Parent entries in the frozen format: {}".format(len(parents)))

    mapChildParent = {}
    tenPercent = max(len(children) // 10, 1)  # 10% of out input data
    for idx, childItem in enumerate(children):
        if idx % tenPercent == 0:
            print("Processed {} of the children data in {} secs...".format(10 * (idx // tenPercent),
                                                                           time.time() - tStart))
        frozenKey = frozenset(childItem[:2])
        parentId = parents.get(frozenKey)
        if parentId is None:
            print("Failed to find parentage information for child: {}".format(childItem))
            continue

        mapChildParent.setdefault(childItem[2], set())
        mapChildParent[childItem[2]].add(parentId)

    print("Final RSS memory footprint: {} kB".format(thisProcess.memory_info().rss / 1024))
    print("Total time creating the parentage information: {}".format(time.time() - tStart))

    # sets are not json serializable
    for keyName in mapChildParent:
        mapChildParent[keyName] = list(mapChildParent[keyName])

    with open("parentage_result.json", "w") as fobj:
        json.dump(mapChildParent, fobj, indent=2)
    print("Result saved on: parentage_result.json")


if __name__ == "__main__":
    sys.exit(main())
