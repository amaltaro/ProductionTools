"""
__checkJobUpdater.py__

Simulates the JobUpdater behaviour. Used only for
debugging purposes.

Created on Apr 21, 2015.
@author: amaltaro
"""
import sys, os
import threading
import logging
#from pprint import pprint
#from optparse import OptionParser
#from collections import defaultdict

from WMCore.Configuration import loadConfigurationFile
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.WMInit import connectToDB
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Lexicon import sanitizeURL

getQuery = """
            SELECT wmbs_workflow.name, wmbs_workflow.task,
                    wmbs_workflow.priority AS workflow_priority,
                    MIN(wmbs_sub_types.priority) AS task_priority
             FROM wmbs_workflow
             INNER JOIN wmbs_subscription ON
               wmbs_workflow.id = wmbs_subscription.workflow
             INNER JOIN wmbs_sub_types ON
               wmbs_subscription.subtype = wmbs_sub_types.id
             GROUP BY wmbs_workflow.name, wmbs_workflow.task,
                      wmbs_workflow.priority"""

def main():
    """
    _main_
    """
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'

    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    # Instantiating central reqmgr and local workqueue
    print "ReqMgr2 URL  : %s" % sanitizeURL(config.JobUpdater.reqMgr2Url)['url']
    print "WorkQueue URL: %s and dbname %s" % (sanitizeURL(config.WorkQueueManager.couchurl)['url'],
                                               config.WorkQueueManager.dbname)

    reqmgr2 = ReqMgr(config.JobUpdater.reqMgr2Url)
    workqueue = WorkQueue(config.WorkQueueManager.couchurl, config.WorkQueueManager.dbname)

    print "\nFirst attempt to update prio of wfs that are not in WMBS and only in local queue"
    priorityCache = {}
    workflowsToUpdate = {}
    workflowsToCheck = [x for x in workqueue.getAvailableWorkflows()]
    print "Retrieved %d workflows from workqueue" % len(workflowsToCheck)

    for workflow, priority in workflowsToCheck:
        if workflow not in priorityCache:
            try:
                priorityCache[workflow] = reqmgr2.getRequestByNames(workflow)[workflow]['RequestPriority']
            except Exception, ex:
                print "Couldn't retrieve the priority of request %s" % workflow
                print "Error: %s" % ex
                continue
        if priority != priorityCache[workflow]:
            workflowsToUpdate[workflow] = priorityCache[workflow]
    print "%d wfs need to be updated in WQ. Final list is: %s" % (len(workflowsToUpdate),
                                                                  workflowsToUpdate)
    for workflow in workflowsToUpdate:
        pass
        # Update priority of a workflow, its spec and all the available elements
        #workqueue.updatePriority(workflow, workflowsToUpdate[workflow])


    print "\nSecond attempt, now check workflows in WMBS"
    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)

    priorityCache = {}
    workflowsToUpdateWMBS = {}
    # Querying "Workflow.ListForJobUpdater"
    output = myThread.transaction.processData(getQuery)
    workflowsToCheck = formatter.formatDict(output)
    print "Retrieved %d workflows from WMBS" % len(workflowsToCheck)

    for workflowEntry in workflowsToCheck:
        workflow = workflowEntry['name']
        if workflow not in priorityCache:
            try:
                priorityCache[workflow] = reqmgr2.getRequestByNames(workflow)[workflow]['RequestPriority']
            except Exception, ex:
                print "Couldn't retrieve the priority of request %s" % workflow
                print "Error: %s" % ex
                continue
        requestPriority = priorityCache[workflow]
        if requestPriority != workflowEntry['workflow_priority']:
            print "Should update priority for %s to %s" % (workflow, requestPriority)
            #workqueue.updatePriority(workflow, priorityCache[workflow])
            print "... then should also update the executing jobs"
            # Check if there are executing jobs for this particular task
            #if executingJobsDAO.execute(workflow, workflowEntry['task']) > 0:
            #    bossAir.updateJobInformation(workflow, workflowEntry['task'],
            #                                      requestPriority = priorityCache[workflow],
            #                                      taskPriority = workflowEntry['task_priority'])
            workflowsToUpdateWMBS[workflow] = priorityCache[workflow]

    print "%d wfs need to be updated in WMBS. Final list is: %s" % (len(workflowsToUpdateWMBS),
                                                                    workflowsToUpdateWMBS)
    if workflowsToUpdateWMBS:
        print "Finally update workflow also in WMBS"
        #updateWorkflowPrioDAO.execute(workflowsToUpdateWMBS)

    print "Done!"
    return 0

if __name__ == '__main__':
    sys.exit(main())
