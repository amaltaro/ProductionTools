#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utilitarian script to delete documents in MongoDB
"""
from __future__ import print_function, division

import sys
import json
import pymongo

def main():
    myClient = pymongo.MongoClient("mongodb://localhost:8230/")
    myDB = myClient["msOutDB"]
    collections = ["msOutRelValColl", "msOutNonRelValColl"]

    summaryByContainer = {}
    for coll in collections:
        if coll == "msOutRelValColl":
            print("RelVals don't run ACDC workflows, so move to the next collection")
            continue
        myCol = myDB[coll]
        resp = myCol.find()
        docs = list(resp)
        print("Found {} documents in the DB collection: {}".format(len(docs), coll))
        for doc in docs:
            for outItem in doc['OutputMap']:
                if outItem['TapeRuleID']:
                    summaryByContainer.setdefault(outItem['Dataset'], [])
                    summaryByContainer[outItem['Dataset']].append(dict(rse=outItem['TapeDestination'],
                                                                       ruleId=outItem['TapeRuleID']))

        print("  there are a total of {} containers with Tape rules".format(len(summaryByContainer)))
        # now filter containers with more than one Tape rule
        for contName in list(summaryByContainer):
            if len(summaryByContainer[contName]) == 1:
                print("Dropping single rule container: {}".format(contName))
                summaryByContainer.pop(contName, None)
        print("  where {} have multiple Tape rules (perhaps dup rules too).".format(len(summaryByContainer)))

        fileName = "/data/user/{}_tapeRules.json".format(coll)
        print("saving summary in file: {}\n".format(fileName))
        with open(fileName, "w") as fObj:
            json.dump(summaryByContainer, fObj, indent=2)

    print("Done!")


if __name__ == '__main__':
    sys.exit(main())
