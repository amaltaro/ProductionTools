"""
__cleanWorkQueue.py__

This script will look for requests in final status and will
check whether:
 1. its spec document has been deleted
 2. its workqueue docs have been deleted
 3. its workqueue_inbox doc has been deleted

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
final_status = ['aborted']

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
    print "Central Couch URL  : %s" % config.WorkloadSummary.couchurl
    print "Central ReqMgr URL  : %s\n" % config.AnalyticsDataCollector.centralRequestDBURL

    wfDBReader = RequestDBReader(config.AnalyticsDataCollector.centralRequestDBURL, 
                                 couchapp = config.AnalyticsDataCollector.RequestCouchApp)

    specDB = Database('reqmgr_workload_cache', config.WorkloadSummary.couchurl)

    wqBackend = WorkQueueBackend(config.WorkloadSummary.couchurl)

    wqInboxDB = Database('workqueue_inbox', config.WorkloadSummary.couchurl)

    for stat in final_status:
        # retrieve list of workflows in each status
        if not wfName:
            finalWfs = wfDBReader.getRequestByStatus([stat])
            print "Found %d wfs in status: %s" %(len(finalWfs), stat)
        else:
            finalWfs = [wfName]
            wfDoc = wfDBReader.getRequestByNames(wfName, True)
            print "Checking %s with status '%s'." % (wfName, wfDoc[wfName]['RequestStatus'])

        for wf in finalWfs:
            # check whether spec file exist
            if specDB.documentExists(wf):
                print "Found spec file for %s" % wf
                # then retrieve the document
                specDoc = specDB.document(wf)

            # check whether there are workqueue docs
            wqDocIDs = wqBackend.getElements(WorkflowName = wf)
            if wqDocIDs:
                print "Found %d workqueue docs for %s" % (len(wqDocIDs), wf)

            # check whether there are workqueue_inbox docs
            if wqInboxDB.documentExists(wf):
                print "Found workqueue_inbox doc for %s" % wf
                # then retrieve the document
                wqInboxDoc = wqInboxDB.document(wf)

    # TODO TODO TODO for the moment only deletes for a specific workflow
    if wfName:
        var = raw_input("\nCan we delete all these documents (Y/N)? ")
        if var == "Y":
            # deletes spec file
            # TODO that is WRONG!!!! I cannot delete this document, only the /spec one
#           if specDoc:
#                print "Deleting spec id %s and %s" % (specDoc['_id'], specDoc['_rev'])
#                specDB.delete_doc(specDoc['_id'], specDoc['_rev'])

            # deletes workqueue_inbox doc
            if wqInboxDoc:
                print "Deleting workqueue_inbox id %s and %s" % (wqInboxDoc['_id'], wqInboxDoc['_rev'])
                wqInboxDB.delete_doc(wqInboxDoc['_id'], wqInboxDoc['_rev'])

            # deletes workqueue docs
            if wqDocIDs:
                print "Deleting workqueue docs %s" % wqDocIDs
                wqBackend.deleteElements(*[x for x in wqDocIDs if x['RequestName'] in wfName])
        else:
            print "You are the boss, aborting it ..."

if __name__ == "__main__":
    sys.exit(main())
