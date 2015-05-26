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

from WMCore.Configuration import loadConfigurationFile
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMComponent.JobCreator.JobCreatorPoller     import retrieveWMSpec

from WMCore.Database.CMSCouch import Database
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend

#TODO: set all these statuses back when testing is finished 
# final_status = ['aborted', 'rejected', 'aborted-completed', 'aborted-archived']
final_status = ['completed']

# TODO: fix the deletion of the spec file
# TODO: implement deletion when there is no input workflow

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

    # Instantiating central services (couch stuff)
#    print "Central Couch URL  : %s" % config.WorkloadSummary.couchurl
#    print "Central ReqMgr URL  : %s\n" % config.AnalyticsDataCollector.centralRequestDBURL

    wfDBReader = RequestDBReader(config.AnalyticsDataCollector.centralRequestDBURL, 
                                 couchapp = config.AnalyticsDataCollector.RequestCouchApp)

    # Central services
    wqBackend = WorkQueueBackend(config.WorkloadSummary.couchurl)
    wqInboxDB = Database('workqueue_inbox', config.WorkloadSummary.couchurl)

    # Local services
    localWQBackend = WorkQueueBackend(config.WorkQueueManager.couchurl, db_name = "workqueue_inbox")
    localWQInboxDB = Database('workqueue', config.WorkQueueManager.couchurl)

    statusList = ["failed", "epic-FAILED", "completed", "closed-out",
                  "announced", "aborted", "aborted-completed", "rejected",
                  "normal-archived", "aborted-archived", "rejected-archived"]

    for stat in final_status:
        # retrieve list of workflows in each status
        if not wfName:
#            options = {'include_docs': False}
            date_range = {'startkey': [2015,5,15,0,0,0], 'endkey': [2015,5,26,0,0,0]}
#            finalWfs = wfDBReader.getRequestByCouchView("bydate", options, date_range)
            tempWfs = wfDBReader.getRequestByCouchView("bydate", date_range)
            #print "Found %d wfs in status: %s" %(len(finalWfs), stat)
            finalWfs = []
            for wf, content in tempWfs.iteritems():
                if content['RequestStatus'] in statusList:
                  finalWfs.append(wf)
            print "Found %d wfs in not in active state" % len(finalWfs)
        else:
            finalWfs = [wfName]
            wfDoc = wfDBReader.getRequestByNames(wfName, True)
            print "Checking %s with status '%s'." % (wfName, wfDoc[wfName]['RequestStatus'])

        for counter, wf in enumerate(finalWfs):
            if counter % 100 == 0:
                print "%d wfs queried ..." % counter
            # check whether there are workqueue docs
            wqDocIDs = wqBackend.getElements(WorkflowName = wf)
            if wqDocIDs:
                print "Found %d workqueue docs for %s, status %s" % (len(wqDocIDs), wf, tempWfs[wf]['RequestStatus'])

            # check whether there are workqueue_inbox docs
            if wqInboxDB.documentExists(wf):
                print "Found workqueue_inbox doc for %s, status %s" % (wf, tempWfs[wf]['RequestStatus'])
                # then retrieve the document
                wqInboxDoc = wqInboxDB.document(wf)

            # check local queue
            wqDocIDs = localWQBackend.getElements(WorkflowName = wf)
            if wqDocIDs:
                print "Found %d local workqueue_inbox docs for %s, status %s" % (len(wqDocIDs), wf, tempWfs[wf]['RequestStatus'])
            if localWQInboxDB.documentExists(wf):
                print "Found local workqueue doc for %s, status %s" % (wf, tempWfs[wf]['RequestStatus'])


    # TODO TODO TODO for the moment only deletes for a specific workflow
    if wfName:
        var = "N" #raw_input("\nCan we delete all these documents (Y/N)? ")
        if var == "Y":
            # deletes workqueue_inbox doc
            if wqInboxDoc:
                print "Deleting workqueue_inbox id %s and %s" % (wqInboxDoc['_id'], wqInboxDoc['_rev'])
                wqInboxDB.delete_doc(wqInboxDoc['_id'], wqInboxDoc['_rev'])

            # deletes workqueue docs
            if wqDocIDs:
                print "Deleting workqueue docs %s" % wqDocIDs
                wqBackend.deleteElements(*[x for x in wqDocIDs if x['RequestName'] in wfName])
        else:
            print "You are the boss, aborting it ...\n"

if __name__ == "__main__":
    sys.exit(main())
