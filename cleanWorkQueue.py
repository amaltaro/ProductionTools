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
from __future__ import print_function

import sys, os
from pprint import pformat
from WMCore.Configuration import loadConfigurationFile

from WMCore.Database.CMSCouch import Database
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend

KEYNAME = 'WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'


def main():
    """
    It will either delete docs in couchdb for the workflow you
    have provided or it will loop over the final (or almost final)
    states and ask for your permission to delete them.
    """
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    if len(sys.argv) != 2:
        print("You must provide an input file name as argument")
        sys.exit(1)

    fileName = sys.argv[1]
    workflowsList = []
    with open(fileName) as fobj:
        lines = fobj.readlines()
        for line in lines:
            workflowsList.append(line.replace('\n', '').strip())
    print("Going to check on {} workflows".format(len(workflowsList)))

    # Central services
    wqBackend = WorkQueueBackend(config.WorkloadSummary.couchurl)
    wqInboxDB = Database('workqueue_inbox', config.WorkloadSummary.couchurl)

    for wfName in workflowsList:
        # check whether there are workqueue_inbox docs
        msg = ""
        if wqInboxDB.documentExists(wfName):
            wqInboxDoc = wqInboxDB.document(wfName)
            #print("Inbox document for {} is\n{}".format(wfName, pformat(wqInboxDoc)))
            msg += "Workflow {} has {} rejected input blocks ".format(wfName, wqInboxDoc[KEYNAME]['RejectedInputs'])
            #print("Inbox document for {} has is\n{}".format(wfName, pformat(wqInboxDoc)))
        else:
            msg += "ERROR: inbox document not found for {} ".format(wfName)

        # check whether there are workqueue docs
        wqDocIDs = wqBackend.getElements(WorkflowName=wfName, returnIdOnly=True)
        msg += "and it has {} workqueue elements".format(len(wqDocIDs))
        #print("    and it has {} workqueue elements".format(len(wqDocIDs)))
        print(msg)


if __name__ == "__main__":
    sys.exit(main())
