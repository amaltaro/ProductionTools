#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utilitarian script to delete documents in MongoDB
"""
from __future__ import print_function, division

import sys
import json
import pymongo
from pprint import pprint

def main():
    myClient = pymongo.MongoClient("mongodb://localhost:8230/")
    myDB = myClient["msOutDB"]
    collections = ["msOutRelValColl", "msOutNonRelValColl"]

    summaryByContainer = {}
    myQuery = {'RequestType': 'Resubmission'}
    for coll in collections:
        myCol = myDB[coll]
        resp = myCol.find(myQuery)
        docs = list(resp)
        print("Found {} documents in the DB collection: {}".format(len(docs), coll))
        for doc in docs:
            for outItem in doc['OutputMap']:
                if outItem['TapeRuleID']:
                    summaryByContainer.setdefault(outItem['Dataset'], [])
                    summaryByContainer[outItem['Dataset']].append(dict(rse=outItem['TapeDestination'],
                                                                       ruleId=outItem['TapeRuleID']))
        print("  and there are {} containers with wrong Tape rules.".format(len(summaryByContainer)))

        fileName = "/data/user/{}_tapeRules.json".format(coll)
        print("saving summary in file: {}\n".format(fileName))
        with open(fileName, "w") as fObj:
            json.dump(summaryByContainer, fObj, indent=2)

    print("Done!")


if __name__ == '__main__':
    sys.exit(main())
