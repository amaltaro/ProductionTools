#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch documents from localhost acdcserver database and post
the same documents to central CouchDB

Preparation:
ssh alancc7-cloud3
cd /data/srv/current/
source apps/reqmgr2/etc/profile.d/init.sh

export X509_USER_KEY=/data/srv/current/auth/couchdb/dmwm-service-key.pem
export X509_USER_CERT=/data/srv/current/auth/couchdb/dmwm-service-cert.pem
"""
from __future__ import print_function, division

import sys
import json
import time
from pprint import pformat, pprint

from WMCore.Database.CMSCouch import Database
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr


def main():
    sys.exit(0)


def fetchRequests():
    """
    Fetch requests in a small set of statuses
    :return: nothing, saves the content in a json file
    """
    reqmgr2Svc = ReqMgr("https://cmsweb.cern.ch/reqmgr2")
    totalReq = []
    for status in ["running-open", "running-closed", "completed"]:
        requests = reqmgr2Svc.getRequestByStatus(status, detail=False)
        print(f"Retrieved {len(requests)} in status: {status}")
        totalReq.extend(requests)

    with open("/data/srv/current/requests.json", "w") as jobj:
        json.dump(totalReq, jobj, indent=2)


if __name__ == '__main__':
    certFile = "/data/srv/current/auth/couchdb/dmwm-service-cert.pem"
    keyFile = "/data/srv/current/auth/couchdb/dmwm-service-key.pem"

    # fetch requests that might need ACDC documents
    # fetchRequests()
    with open("/data/srv/current/requests.json") as jobj:
        requests = json.load(jobj)
    print(f"Loaded a total of {len(requests)}")


    localDB = Database(dbname="acdcserver", url="http://localhost:5984", cert=certFile, ckey=keyFile)
    centralDB = Database(dbname="acdcserver", url="https://cmsweb.cern.ch:8443/couchdb", cert=certFile, ckey=keyFile)

    docsCreated = 0
    dumpData = []
    for wfName in requests:
        time.sleep(2)
        print("")
        localDocs = localDB.loadView(design="ACDC", view="byCollectionName",
                                     options={"key": wfName, "stale": "ok", "reduce": False})["rows"]
        if not localDocs:
            print(f"Workflow {wfName} has no documents in the ACDCServer")
            dumpData.append(dict(wflow=wfName, doc_ids=[]))
            continue

        centralDocs = centralDB.loadView(design="ACDC", view="byCollectionName",
                                         options={"key": wfName, "stale": "ok", "reduce": False})["rows"]
        localIds = set([item["id"] for item in localDocs])
        centralIds = set([item["id"] for item in centralDocs])
        missingIds = list(localIds - centralIds)
        #print(f"Total of {len(missingIds)} missing docs, they are: {missingIds}")
        print(f"Total of {len(missingIds)} missing docs")
        if not missingIds:
            print(f"Workflow {wfName} has no missing documents in the ACDCServer")
            dumpData.append(dict(wflow=wfName, doc_ids=[]))
            continue

        print(f"Workflow {wfName} has {len(localDocs)} local and {len(centralDocs)} central docs.")
        #print(f"Local docs: {pformat(localDocs)}")
        #print(f"Central docs: {pformat(centralDocs)}")

        localDocs = localDB.loadView(design="ACDC", view="byCollectionName",
                                     options={"key": wfName, "stale": "ok", "reduce": False, "include_docs": True})["rows"]

        if not localDocs:
            print(f"Failed to retrieve full documents from central ACDC")
            dumpData.append(dict(wflow=wfName, doc_ids=[]))
            continue
        for docs in localDocs:
            docs["doc"].pop("_rev")
            centralDB.queue(docs["doc"])
        resp = centralDB.commit()
        print(f"Commited {len(resp)} documents for: {wfName}")
        dumpData.append(dict(wflow=wfName, doc_ids=missingIds))

    print(f"\nCopied a total of {docsCreated} documents to the central ACDCServer")
    print("Creating summary file with workflow and doc ids")
    with open("/data/srv/current/summary.json", "w") as jobj:
        json.dump(dumpData, jobj, indent=2)
    print("Done!")
    # print(f"Commit response for workflow {wfName} was: {resp}")

