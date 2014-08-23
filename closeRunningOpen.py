"""
_closeRunningOpen_

Use it when many requests are stuck in running-open probably because of problems
in the GQ.

Created on Jul 9, 2013
Updated on Aug 23, 2014 by Alan

@author: dballest
"""
#TODO TODO TODO: need to tested, especially the update part

import sys

from WMCore.Database.CMSCouch import Database
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend

def getProblematicRequests():
    """
    _getProblematicRequests_
    """
    badWorkflows = []
    backend = WorkQueueBackend('https://cmsweb.cern.ch/couchdb')
    workflowsToCheck = backend.getInboxElements(OpenForNewData = True)
    for element in workflowsToCheck:
        childrenElements = backend.getElementsForParent(element)
        if not len(childrenElements):
            badWorkflows.append(element)
    return badWorkflows

def main():
    print "Looking for problematic inbox elements..."
    problemRequests = getProblematicRequests()
    print "Found %d bad elements:" % len(problemRequests)
    if not problemRequests:
        print "Nothing to fix, contact a developer if the problem persists..."
        return 0
    for request in problemRequests:
        print request["RequestName"]
    var = raw_input("Can we close these for new data in inbox elements: Y/N\n")
    if var == "Y":
        print "Updating them in global inbox, you need a WMAgent proxy for this."
        inboxDB = Database('workqueue_inbox', 'https://cmsweb.cern.ch/couchdb')
        for request in problemRequests:
            inboxDB.document(request._id)
            inboxDB.updateDocument(request._id, 'WorkQueue', 'in-place', fields={'OpenForNewData': false})
        print "Done with the deletions, this should fix the problem."
        return 0
    else:
        var = raw_input("Then can we delete these inbox elements: Y/N\n")
        if var == "Y":
            print "Deleting them from the global inbox, you need a WMAgent proxy for this."
            inboxDB = Database('workqueue_inbox', 'https://cmsweb.cern.ch/couchdb')
            for request in problemRequests:
                inboxDB.delete_doc(request._id, request.rev)
            print "Done with the deletions, this should fix the problem."
            return 0
        else:
            print "Doing nothing as you commanded..."
        return 0

if __name__ == "__main__":
    sys.exit(main())
