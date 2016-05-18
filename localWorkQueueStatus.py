"""
__localWorkQueueStatus.py__

It gives you an overview of the local workqueue and workqueue_inbox database
focusing on the elements status.

Created on Apr 27, 2015.
@author: amaltaro
"""

import sys
import os
import logging
from pprint import pformat

from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def createElementsSummary(allElements, dbName):
    """
    Print the local couchdb situation based on the WQE status
    """
    summary = {}
    for elem in allElements:
        summary.setdefault(elem['Status'], 0)
        summary[elem['Status']] += 1
    logger.info("Found a total of %d elements in the '%s' db", len(allElements), dbName)
    logger.info(pformat(summary))


def main():
    """
    Whatever
    """
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    # Get local workqueue and workqueue_inbox docs
    localWQBackend = WorkQueueBackend(config.WorkQueueManager.couchurl, db_name="workqueue")
    localWQInboxDB = WorkQueueBackend(config.WorkQueueManager.couchurl, db_name="workqueue_inbox")
    wqDocIDs = localWQBackend.getElements()
    wqInboxDocIDs = localWQInboxDB.getElements()

    # Build and print a summary of these elements
    createElementsSummary(wqDocIDs, 'workqueue')
    createElementsSummary(wqInboxDocIDs, 'workqueue_inbox')

    # Now investigate only Available docs in the workqueue database
    availableElem = [x for x in wqDocIDs if x['Status'] == 'Available']
    stuckElements = []
    workSplitBySite = {}   # we divide the number of jobs by the number of common sites
    workBySite = {}        # we simply add Jobs to each of the common sites
    workOverview = {'totalGoodJobs': 0, 'totalBadJobs': 0, 'totalAvailableGoodLQE': 0, 'totalAvailableBadLQE': 0}

    for elem in availableElem:
        if elem.get('NoLocationUpdate'):
            commonSites = set(elem['SiteWhitelist'])
            logger.debug("NoLocationUpdate element assigned to %s", commonSites)
        elif elem['NoInputUpdate'] and elem['NoPileupUpdate']:
            commonSites = set(elem['SiteWhitelist'])
            logger.debug("NoInputUpdate AND NoPileupUpdate element assigned to %s", commonSites)
        elif elem['NoInputUpdate']:
            puSites = elem['PileupData'].values()[0] if elem['PileupData'] else elem['SiteWhitelist']
            commonSites = set(puSites) & set(elem['SiteWhitelist'])
            logger.debug("NoInputUpdate element with pileup location and sitewhitelist intersection as: %s", commonSites)
        elif elem['NoPileupUpdate']:
            inputSites = elem['Inputs'].values()[0] if elem['Inputs'] else elem['SiteWhitelist']
            commonSites = set(inputSites) & set(elem['SiteWhitelist'])
            logger.debug("NoPileupUpdate element with input location and sitewhitelist intersection as: %s", commonSites)
        else:
            inputSites = elem['Inputs'].values()[0] if elem['Inputs'] else elem['SiteWhitelist']
            puSites = elem['PileupData'].values()[0] if elem['PileupData'] else elem['SiteWhitelist']
            commonSites = set(inputSites) & set(puSites) & set(elem['SiteWhitelist'])
            logger.debug("NoPileupUpdate element with input location and sitewhitelist intersection as: %s", commonSites)

        if not commonSites:
            workOverview['totalBadJobs'] += elem['Jobs']
            workOverview['totalAvailableBadLQE'] += 1
            tempElem = {'RequestName': elem['RequestName'],
                        'id': elem.id,
                        'NoLocationUpdate': elem.get('NoLocationUpdate'),
                        'NoInputUpdate': elem['NoInputUpdate'],
                        'NoPileupUpdate': elem['NoPileupUpdate'],
                        'Inputs': elem['Inputs'].values()[0] if elem['Inputs'] else [],
                        'PileupData': elem['PileupData'].values()[0] if elem['PileupData'] else [],
                        'SiteWhitelist': elem['SiteWhitelist']}
            # now get the location where it was supposed to be
            inboxDoc = localWQInboxDB.getElements(elementIDs=[elem.id])[0]
            tempElem['OriginalInputLocation'] = inboxDoc['Inputs']
            stuckElements.append(tempElem)
        else:
            workOverview['totalGoodJobs'] += elem['Jobs']
            workOverview['totalAvailableGoodLQE'] += 1
            for site in commonSites:
                workSplitBySite.setdefault(site, 0)
                workSplitBySite[site] += elem['Jobs']/len(commonSites)
                workBySite.setdefault(site, {'Jobs': 0, 'LQE': 0})
                workBySite[site]['Jobs'] += elem['Jobs']
                workBySite[site]['LQE'] += 1
                #if site == 'T2_CH_CERN':
                #    logging.info("%s, id %s with %d jobs to process", elem['RequestName'], elem.id, elem['Jobs'])

    # Report on site vs jobs vs elements situation
    logger.info("AGENT OVERVIEW: %s\n", pformat(workOverview))
    logger.info("Average of jobs per site (equally divides jobs among the common sites):\n%s\n", pformat(workSplitBySite))
    logger.info("Jobs per site (do not divide jobs among the common sites):\n%s\n", pformat(workBySite))

    logger.info("Found %d elements stuck in Available in local workqueue with no common site/data location:", len(stuckElements))
    for elem in stuckElements:
        logger.info("    %s with docid %s", elem['RequestName'], elem['id'])
    logger.debug(pformat(stuckElements))

    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())
