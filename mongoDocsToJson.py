#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utilitarian script to retrieve all the documents in a database + collection,
and dump them in a json file

NOTE: in order to get pymongo in the path, you need to source the reqmgr2ms init.sh script
source /data/srv/current/sw/slc7_amd64_gcc630/cms/reqmgr2ms/0.4.5.pre3/etc/profile.d/init.sh
"""
from __future__ import print_function, division

import json
import sys

import pymongo


def main():
    myClient = pymongo.MongoClient("mongodb://localhost:8230/")
    myDB = myClient["msOutDB"]
    collections = ["msOutRelValColl", "msOutNonRelValColl"]

    for coll in collections:
        myCol = myDB[coll]
        resp = myCol.find()
        docs = list(resp)
        print("Found {} documents in the DB collection: {}".format(len(docs), coll))

        fileName = "/data/user/{}.json".format(coll)
        print("saving summary in file: {}\n".format(fileName))
        with open(fileName, "w") as fObj:
            json.dump(docs, fObj, indent=2)

    print("Done!")


if __name__ == '__main__':
    sys.exit(main())
