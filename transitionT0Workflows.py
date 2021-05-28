#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script used to make request status transition for T0 workflows
"""
from __future__ import print_function, division

import logging
import os
import sys

from WMCore.Configuration import loadConfigurationFile
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


def main():
    wflowName = "Repack_Run341695_StreamPhysics_Tier0_REPLAY_2021_p4v1_v2105180358_210518_0358"

    dictMap = {"new": "Closed",
               "Closed": "completed",
               "completed": "normal-archived"}

    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/tier0/srv/wmagent/current/config/tier0/config.py'
    # config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])
    # config.AnalyticsDataCollector.centralRequestDBURL = 'https://cmsweb.cern.ch/couchdb/t0_request'
    # config.AnalyticsDataCollector.RequestCouchApp = 'T0Request'

    centralRequestDBURL = 'https://cmsweb.cern.ch/couchdb/t0_request'
    RequestCouchApp = "T0Request"

    logger = setupLogger()
    reqmgrDB = RequestDBWriter(centralRequestDBURL, couchapp=RequestCouchApp)

    reqDict = reqmgrDB.getRequestByNames(wflowName)
    currentStatus = reqDict[wflowName]['RequestStatus']
    logger.info("Current status for workflow %s is: %s", wflowName, currentStatus)
    if reqDict[wflowName]['RequestStatus'] not in dictMap:
        logger.info("Request %s is already in final state: %s", wflowName, currentStatus)
        sys.exit(0)

    while True:
        logger.info("Transitioning request %s from '%s' to '%s'", wflowName, currentStatus, dictMap[currentStatus])
        response = reqmgrDB.updateRequestStatus(wflowName, dictMap[currentStatus])
        logger.info("response from the server: %s", response)

        if dictMap[currentStatus] not in dictMap:
            logger.info("There is no further status transition to happen")
            break

    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
