#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Scan two directories and prints the name of the files found in both directories
"""
from __future__ import print_function, division
import sys
import os
from pprint import pprint, pformat

def main():
    mainDir = "/Users/amaltaro/Pictures/Bento/"
    listDirs = os.listdir(mainDir)
    filesByDir = dict()
    for dirName in listDirs:
        if dirName.startswith("Mar"):
            filesByDir[dirName] = os.listdir(mainDir + dirName)
    print("Summary of pictures by directory:")
    for k, v in filesByDir.items():
        print("  {} : {}".format(k, len(v)))
    print("\nChecking for duplicate files...")
    res = set(filesByDir['Mar2017_2018']) & set(filesByDir['Mar2018_2019'])
    print("  Between Mar2017_2018 and Mar2017_2018: ".format(res))

    res = set(filesByDir['Mar2018_2019']) & set(filesByDir['Mar2019_2020'])
    print("  Between Mar2018_2019 and Mar2019_2020: ".format(res))

    res = set(filesByDir['Mar2019_2020']) & set(filesByDir['Mar2020_2021'])
    print("  Between Mar2019_2020 and Mar2020_2021: ".format(res))

    res = set(filesByDir['Mar2020_2021']) & set(filesByDir['Mar2021_2022'])
    print("  Between Mar2020_2021 and Mar2021_2022: ".format(res))
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
