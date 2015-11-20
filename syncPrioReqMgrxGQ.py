"""
__syncPrioReqMgrxGQ.py__

This script retrieves the RequestPriority in ReqMgr and then propagates it to
all Available workqueue docs in the GQ.

Created on Nov 20, 2015.
@author: amaltaro
"""

import sys
import os

from WMCore.Configuration import loadConfigurationFile
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader

from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend

def main():
    """
    It will either delete docs in couchdb for the workflow you
    have provided or it will loop over the final (or almost final)
    states and ask for your permission to delete them.
    """
    args = sys.argv[1:]
    if not len(args) == 1:
        print "usage: python syncPrioReqMgrxGQ.py <text_file_with_the_workflow_names>"
        sys.exit(0)
    inputFile = args[0]
    with open(inputFile) as f:
      listWorkflows = [x.rstrip('\n') for x in f.readlines()]

    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'

    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    wfDBReader = RequestDBReader(config.AnalyticsDataCollector.centralRequestDBURL, 
                                 couchapp = config.AnalyticsDataCollector.RequestCouchApp)

    wqBackend = WorkQueueBackend(config.WorkloadSummary.couchurl)

    workflowsDict = wfDBReader.getRequestByNames(listWorkflows)

    for wf, details in workflowsDict.iteritems():
        print "wf: %s and prio: %s" % (wf, details['RequestPriority'])
        wqDocs = wqBackend.getElements(WorkflowName = wf)
        docIds = [elem._id for elem in wqDocs if elem['Status'] == 'Available' and elem['Priority'] != details['RequestPriority']]
        if docIds:
            print "Changing the priority of the following available docs: %s" % docIds
            wqBackend.updateElements(*docIds, Priority=details['RequestPriority'])
        else:
            print " there is nothing to update for this workflow."


if __name__ == "__main__":
    sys.exit(main())
