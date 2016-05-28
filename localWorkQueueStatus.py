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
import argparse
from pprint import pformat

from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend


parser = argparse.ArgumentParser(description="Local workqueue monitoring")
parser.add_argument('-v', '--verbose', help='Increase output verbosity', action='store_true')
args = parser.parse_args()

if args.verbose:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)


def createElementsSummary(allElements, dbName):
    """
    Print the local couchdb situation based on the WQE status
    """
    summary = {}
    for elem in allElements:
        summary.setdefault(elem['Status'], 0)
        summary[elem['Status']] += 1
    logging.info("Found a total of %d elements in the '%s' db", len(allElements), dbName)
    logging.info(pformat(summary))
    return summary.keys()


def byStatusSummary(elemByStatus, localWQInboxDB=None):
    """
    Parse each element to build up a summary based on the element status.
    """
    stuckElements = []
    workSplitBySite = {}   # we divide the number of jobs by the number of common sites
    workBySite = {}        # we simply add Jobs to each of the common sites
    workOverview = {'totalGoodJobs': 0, 'totalBadJobs': 0, 'totalAvailableGoodLQE': 0, 'totalAvailableBadLQE': 0}

    for elem in elemByStatus:
        if elem.get('NoLocationUpdate'):
            commonSites = set(elem['SiteWhitelist'])
            logging.debug("NoLocationUpdate element assigned to %s", commonSites)
        elif elem['NoInputUpdate'] and elem['NoPileupUpdate']:
            commonSites = set(elem['SiteWhitelist'])
            logging.debug("NoInputUpdate AND NoPileupUpdate element assigned to %s", commonSites)
        elif elem['NoInputUpdate']:
            puSites = elem['PileupData'].values()[0] if elem['PileupData'] else elem['SiteWhitelist']
            commonSites = set(puSites) & set(elem['SiteWhitelist'])
            logging.debug("NoInputUpdate element with pileup location and sitewhitelist intersection as: %s", commonSites)
        elif elem['NoPileupUpdate']:
            inputSites = elem['Inputs'].values()[0] if elem['Inputs'] else elem['SiteWhitelist']
            commonSites = set(inputSites) & set(elem['SiteWhitelist'])
            logging.debug("NoPileupUpdate element with input location and sitewhitelist intersection as: %s", commonSites)
        else:
            inputSites = elem['Inputs'].values()[0] if elem['Inputs'] else elem['SiteWhitelist']
            puSites = elem['PileupData'].values()[0] if elem['PileupData'] else elem['SiteWhitelist']
            commonSites = set(inputSites) & set(puSites) & set(elem['SiteWhitelist'])
            logging.debug("Unflagged element with input location and sitewhitelist intersection as: %s", commonSites)

        if not commonSites:
            workOverview['totalBadJobs'] += elem['Jobs']
            workOverview['totalAvailableBadLQE'] += 1
            tempElem = {'RequestName': elem['RequestName'],
                        'id': elem.id,
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
    logging.info("Average number of UNIQUE jobs per site:\n%s\n", pformat(workSplitBySite))
    logging.info("Maximum number of POSSIBLE jobs per site:\n%s\n", pformat(workBySite))

    if elemByStatus[0]['Status'] == 'Available':
        logging.info("Found %d elements stuck in Available in local workqueue with no common site/data location:", len(stuckElements))
        for elem in stuckElements:
            logging.info("  %s with docid %s, site whitelist set to %s while input %s only available at %s", elem['RequestName'],
                                                                                                             elem['id'],
                                                                                                             elem['SiteWhitelist'],
                                                                                                             elem['OriginalInputLocation'].keys(),
                                                                                                             elem['Inputs'])
        logging.debug("%s\n", pformat(stuckElements))


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
    logging.info("************* workqueue elements summary ************")
    foundStatus = createElementsSummary(wqInboxDocIDs, 'workqueue_inbox')
    foundStatus = createElementsSummary(wqDocIDs, 'workqueue')

    # Now investigate docs in the workqueue database
    for status in foundStatus:
        logging.info("\n************* workqueue elements summary by status: %s ************", status)
        elemByStatus = [x for x in wqDocIDs if x['Status'] == status]
        byStatusSummary(elemByStatus, localWQInboxDB=localWQInboxDB)

    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())
