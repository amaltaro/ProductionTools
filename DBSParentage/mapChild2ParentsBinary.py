#!/usr/bin/env python
"""
Ssript to parse two json files:
 * one with parent dataset level run - lumi - fileid information
 * the other with child dataset (or block) level run - lumi - fileid information
and using the lumi section number (and run number), maps children to parent files.
"""
from __future__ import print_function, division

import json
import operator
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


def sortInput(parents, children):
    """Sort parents by lumi and run; sort children by file id"""
    tStart = time.time()
    parents = sorted(parents, key=operator.itemgetter(1, 0))
    children = sorted(children, key=operator.itemgetter(2))
    print("Time sorting parent/children data: {}".format(time.time() - tStart))
    return parents, children


def contains(elements, value):
    """
    Search for the run/lumi tuple in the parents data structure, which is like:
    [[1, 1, 553945320], [1, 2, 553945320], [1, 3, 553945320], ...]
    thus: run_number, lumi_section, file_id

    Elements corresponds to the original parentage data structure; while value
    is a run/lumi tuple

    Code snippet from:
    https://realpython.com/binary-search-python/#implementing-binary-search-in-python
    """
    left, right = 0, len(elements) - 1

    if left <= right:
        middle = (left + right) // 2

        if elements[middle][1] == value[1]:
            #        print("DEBUG found same lumi for middle: {}".format(elements[middle]))
            if elements[middle][0] == value[0]:
                return elements[middle][2]
            elif elements[middle][0] < value[0]:
                return contains(elements[middle + 1:], value)
            elif elements[middle][0] > value[0]:
                return contains(elements[:middle], value)

        if elements[middle][1] < value[1]:
            return contains(elements[middle + 1:], value)
        elif elements[middle][1] > value[1]:
            return contains(elements[:middle], value)
    return False


def main():
    # make it a different function such that gc can collect objects
    tStart = time.time()
    thisProcess = psutil.Process(os.getpid())
    print("Initial RSS memory footprint: {} kB".format(thisProcess.memory_info().rss / 1024))
    parents, children = loadFiles()
    print("Found {} parent entries".format(len(parents)))
    print("Found {} children entries".format(len(children)))

    parents, children = sortInput(parents, children)
    print("Sorted {} parent entries".format(len(parents)))
    print("Sorted {} children entries".format(len(children)))

    print("DEBUG: Parents head: {}\n\tParents tail: {}".format(parents[:5], parents[-5:]))
    print("DEBUG: Children head: {}\n\tChildren tail: {}".format(children[:5], children[-5:]))

    mapChildParent = {}
    tenPercent = max(len(children) // 10, 1)  # 10% of out input data
    for idx, childItem in enumerate(children):
        if idx % tenPercent == 0:
            print("Processed {}% of the children data in {} secs...".format(10 * (idx // tenPercent),
                                                                            time.time() - tStart))
        # print("DEBUG searching for: {}".format(childItem))
        mapChildParent.setdefault(childItem[2], set())
        # now ask for a [run, lumi] information
        fileid = contains(parents, value=childItem[:2])
        if not fileid:
            print("Failed to find parentage information for children tuple: {}".format(childItem))
            continue
        mapChildParent[childItem[2]].add(fileid)
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
