#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
NOTE: Use this script with care, otherwise it can mess up a given workflow

This script can be used when there is a workqueue element acquired by an
agent, but stuck in Available in local workqueue db.
Expected actions are:
1. set the local workqueue_inbox element to Available (plus other parameters that need to be reset)
2. delete the corresponding local workqueue element.
"""
from __future__ import print_function

import os
import sys
import logging
from pprint import pprint
from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend

def setupLogger():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)
    return root


def main():
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    print("Work in progress! It might create document conflicts as it is!")
    sys.exit(10)

    if len(sys.argv) != 2:
        print("You must provide a request name")
        sys.exit(1)
    reqName = sys.argv[1]
    childQueue = config.WorkQueueManager.queueParams['QueueURL']

    logger = setupLogger()
    localWQBackend = WorkQueueBackend(config.WorkQueueManager.couchurl, db_name="workqueue", logger=logger)
    localElems = localWQBackend.getElements(WorkflowName=reqName)
    localInboxElems = localWQBackend.getInboxElements(WorkflowName=reqName)

    docsToUpdate = []
    logger.info("** Local workqueue_inbox elements for workflow %s and agent %s", reqName, childQueue)
    for elem in localInboxElems:
        if elem['Status'] == "Acquired":
            logger.info("Element id: %s has status: %s", elem.id, elem['Status'])
            elem['Status'] = 'Available'
            elem['ChildQueueUrl'] = None
            docsToUpdate.append(elem)
    if docsToUpdate:
        var = raw_input("Found %d inbox elements to update, shall we proceed (Y/N): " % len(docsToUpdate))
        if var == "Y":
            resp = localWQBackend.saveElements(*docsToUpdate)
            logger.info("    update response: %s", resp)

    docsToUpdate = []
    logger.info("** Local workqueue elements for workflow %s and agent %s", reqName, childQueue)
    for elem in localElems:
        if elem['Status'] == "Available":
            logger.info("Element id: %s has status: %s", elem.id, elem['Status'])
            docsToUpdate.append(elem._id)
    if docsToUpdate:
        var = raw_input("Found %d elements to delete, shall we proceed (Y/N): " % len(docsToUpdate))
        if var == "Y":
            for elem in docsToUpdate:
                elem.delete()
            resp = docsToUpdate[0]._couch.commit()
            logger.info("    deletion response: %s", resp)

    print("Done!")

    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())
