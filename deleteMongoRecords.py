#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utilitarian script to delete documents in MongoDB
"""
from __future__ import print_function, division

import sys

import pymongo


# { "_id" : "amaltaro_SC_LumiMask_PhEDEx_HG2009_Val_200908_151648_1493" }


def main():
    docIds = ["cmsunified_ACDC0_task_EXO-RunIIAutumn18DRPremix-05022__v1_T_200923_122119_7219",
              "cmsunified_ACDC0_task_EXO-RunIIFall18wmLHEGS-03451__v1_T_200923_121106_381"]

    myClient = pymongo.MongoClient("mongodb://localhost:8230/")
    myDB = myClient["msOutDB"]
    collections = ["msOutRelValColl", "msOutNonRelValColl"]

    # resp = mycol.find(myquery)
    for coll in collections:
        print("Going to delete documents in the collection: {}".format(coll))
        myCol = myDB[coll]
        for docName in docIds:
            myQuery = {"RequestName": docName}
            resp = myCol.find_one_and_delete(myQuery)
            if resp:
                print("    Deletion result for {}: {}".format(docName, resp["_id"]))
            else:
                print("    Deletion result for {}: {}".format(docName, resp))


if __name__ == '__main__':
    sys.exit(main())
