#!/usr/bin/env python
"""
Script to convert a text file in the format of:
    run_num     lumi_section    file_id
    ...
to a json file in the format of:
[(run1, lumi1, file1),
 (run2, lumi2, file2), ...]
"""
from __future__ import print_function

import json
import sys


def main():
    if len(sys.argv) != 2:
        print("You must provide the input file name!")
        sys.exit(1)

    fname = sys.argv[1]
    finalData = []
    with open(fname) as fdesc:
        for line in fdesc.readlines():
            if not line:
                continue
            try:
                run, lumi, fileid = line.split()
                lineTuple = (int(run), int(lumi), int(fileid))
            except ValueError:
                print("Line skipped: {}".format(line))
                continue
            finalData.append(lineTuple)

    print("final data has {} items".format(len(finalData)))
    newName = fname.replace(".sql", ".json")
    with open(newName, "w") as fobj:
        json.dump(finalData, fobj, indent=2)
    print("Created {} file with the json data".format(newName))


if __name__ == "__main__":
    sys.exit(main())
