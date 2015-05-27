"""
__cleanWorkQueue.py__

This script will look for requests in final status and will
check whether:
 1. its workqueue docs have been deleted (in central couch)
 2. its workqueue_inbox doc has been deleted (in central couch)

In case it finds these documents, then it will ask for their deletion.

Created on Apr 27, 2015.
@author: amaltaro
"""

import sys, os
from pprint import pprint
from WMCore.Configuration import loadConfigurationFile
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMComponent.JobCreator.JobCreatorPoller     import retrieveWMSpec

from WMCore.Database.CMSCouch import Database
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend

def main():
    """
    It will either delete docs in couchdb for the workflow you
    have provided or it will loop over the final (or almost final)
    states and ask for your permission to delete them.
    """
    wfName = sys.argv[1] if len(sys.argv) == 2 else []

    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'

    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    wfDBReader = RequestDBReader(config.AnalyticsDataCollector.centralRequestDBURL, 
                                 couchapp = config.AnalyticsDataCollector.RequestCouchApp)

    # Local services
    localWQBackend = WorkQueueBackend(config.WorkQueueManager.couchurl, db_name = "workqueue_inbox")
    localWQInboxDB = Database('workqueue', config.WorkQueueManager.couchurl)

    elemKeys = ['Jobs','RequestName', 'Priority', 'SiteWhitelist', 'Status', 'TaskName', 'ParentQueueId']

    # check local queue
    wqDocIDs = localWQBackend.getElements()
    print "Found %d local workqueue_inbox docs.\n" % len(wqDocIDs)

    # print the whole stuff
    filteredElemts = []
    for ele in wqDocIDs:
        #temp = { k:ele[k] for k in elemKeys }
        temp = {}
        for k in elemKeys:
            temp[k] = ele[k]
        filteredElemts.append(temp)
#        print temp

    # Getting total number of jobs per workqueue docs status
    STATES = ('Acquired', 'Running', 'Done', 'Failed', 'CancelRequested', 'Canceled')
    wqStatus = {}
    for i in filteredElemts:
        if i['Status'] not in wqStatus:
            wqStatus[i['Status']] = {'Jobs': i['Jobs'], 'ElemCounter': 1}
        else:
            wqStatus[i['Status']]['Jobs'] += i['Jobs']
            wqStatus[i['Status']]['ElemCounter'] += 1
    print "Overview of workqueue_inbox elements"
    pprint(wqStatus)


    # ordering by workflow name
    orderedElemts = {}
    for i in filteredElemts:
        if i['RequestName'] not in orderedElemts:
            orderedElemts[i['RequestName']] = [i]
        else:
            orderedElemts[i['RequestName']].append(i)
        del orderedElemts[i['RequestName']][-1]['RequestName']
    print "\nOrdering workqueue elements by workflow"
    pprint(orderedElemts)
#    if localWQInboxDB.documentExists(wf):
#        print "Found local workqueue doc for %s, status %s" % (wf, tempWfs[wf]['RequestStatus'])

if __name__ == "__main__":
    sys.exit(main())
