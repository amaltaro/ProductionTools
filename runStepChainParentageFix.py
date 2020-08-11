#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Does a run similar to the ReqMgr2 CherryPy thread: StepChainParentageFixTask.py
"""
from __future__ import print_function, division

import logging
import sys
from collections import defaultdict

from WMCore.Services.DBS.DBS3Reader import DBS3Reader
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter


def setupLogger():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)
    return root


def getChildDatasetsForStepChainMissingParent(reqmgrDB, status, logger):
    results = reqmgrDB.getStepChainDatasetParentageByStatus(status)

    requestsByChildDataset = defaultdict(set)

    for reqName, info in results.items():
        ### FIXME TODO fix only this one
        # if reqName != "cmsunified_task_TSG-Phase2HLTTDRWinter20GS-00091__v1_T_200219_195740_3852":
        #    continue
        ### done hacking
        logger.info("Request: %s needs fixing", reqName)
        for dsInfo in info.values():
            if dsInfo["ParentDset"]:
                for childDS in dsInfo["ChildDsets"]:
                    requestsByChildDataset[childDS].add(reqName)
    return requestsByChildDataset


def main():
    BASE_URL = "https://cmsweb.cern.ch"
    COUCH_URL = "%s/couchdb" % BASE_URL

    dbs_url = "https://cmsweb.cern.ch/dbs/prod/global/DBSWriter"
    reqmgrdb_url = "%s/reqmgr_workload_cache" % (COUCH_URL)
    statusToCheck = ['closed-out', 'announced']

    logger = setupLogger()
    dbsSvc = DBS3Reader(dbs_url, logger=logger)
    reqmgrDB = RequestDBWriter(reqmgrdb_url)

    logger.info("Running fixStepChainParentage thread for statuses")
    childDatasets = set()
    requests = set()
    requestsByChildDataset = {}
    for status in statusToCheck:
        reqByChildDS = getChildDatasetsForStepChainMissingParent(reqmgrDB, status, logger)
        logger.info("Retrieved %d datasets to fix parentage, in status: %s",
                    len(reqByChildDS), status)
        childDatasets = childDatasets.union(set(reqByChildDS.keys()))
        # We need to just get one of the StepChain workflow if multiple workflow contains the same datasets. (i.e. ACDC)
        requestsByChildDataset.update(reqByChildDS)

        for wfs in reqByChildDS.values():
            requests = requests.union(wfs)
    logger.info("  datasets are: %s", reqByChildDS)

    failedRequests = set()
    totalChildDS = len(childDatasets)
    fixCount = 0
    for childDS in childDatasets:
        logger.info("Resolving parentage for dataset: %s", childDS)
        try:
            failedBlocks = dbsSvc.fixMissingParentageDatasets(childDS, insertFlag=True)
        except Exception as exc:
            logger.exception("Failed to resolve parentage data for dataset: %s. Error: %s",
                             childDS, str(exc))
            failedRequests = failedRequests.union(requestsByChildDataset[childDS])
        else:
            if failedBlocks:
                logger.warning("These blocks failed to be resolved and will be retried later: %s",
                               failedBlocks)
                failedRequests = failedRequests.union(requestsByChildDataset[childDS])
            else:
                fixCount += 1
                logger.info("Parentage for '%s' successfully updated. Processed %s out of %s datasets.",
                            childDS, fixCount, totalChildDS)
        logger.info("    dataset sorted: %s\n", childDS)

    requestsToUpdate = requests - failedRequests

    ### FIXME: disable the block below if you do NOT want to update the
    # workflow in ReqMgr2
    for request in requestsToUpdate:
        try:
            reqmgrDB.updateRequestProperty(request, {"ParentageResolved": True})
            logger.info("Marked ParentageResolved=True for request: %s", request)
        except Exception as exc:
            logger.error("Failed to update 'ParentageResolved' flag to True for request: %s", request)

    msg = "A total of %d requests have been processed, where %d will have to be retried in the next cycle."
    logger.info(msg, len(requestsToUpdate), len(failedRequests))

    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
