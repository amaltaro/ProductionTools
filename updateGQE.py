"""
_updateGQE_

Can be converted to a generic script that updates global workqueue
elements, provided some filters and which property to be updated.

For this specific case, it looks for GQE in status Running and which
are older than EPOCH time. If the GQE was updated before that EPOCH
time, then it was still marked as Running when the agent got redeployed.

Run it from the agent, with the agent environment loaded
"""
from __future__ import print_function

import socket
import sys
import time

from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend

backend = WorkQueueBackend('https://cmsweb.cern.ch/couchdb')


def getProblematicRequests(epochT):
    """
    _getProblematicRequests_
    """
    oldElems = []
    agentUrl = "http://" + socket.gethostname() + ":5984"
    print("Going to look for problematic elements for agent %s and epoch time %s" % (agentUrl, epochT))
    elements = backend.getElements(status="Running", ChildQueueUrl=agentUrl)
    for elem in elements:
        if float(elem.updatetime) > float(epochT):
            # element in the new agent, all good!
            continue
        updatedIn = time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.localtime(float(elem.updatetime)))
        print("id: %s\tRequestName: %s\tStatus: %s\t\tUpdateIn: %s" % (elem.id, elem['RequestName'], elem['Status'], updatedIn))
        oldElems.append(elem)
    print("Found %d old elements out of %d Running in %s" % (len(oldElems), len(elements), agentUrl))
    return oldElems


def main():
    if len(sys.argv) != 2:
        print("You must provide an EPOCH time as argument.")
        print("E.g.: python updateGQE.py 1550051866")
        sys.exit(1)
    epochT = sys.argv[1]
    problemElems = getProblematicRequests(epochT)
    print("Found %d bad elements that needs fixup" % len(problemElems))
    if not problemElems:
        print("Nothing to fix, contact a developer if the problem persists...")
        return 0

    var = raw_input("\nCan we mark those elements with status Done (Y/N): ")
    if var == "Y":
        elemIds = [elem.id for elem in problemElems]
        backend.updateElements(*elemIds, Status="Done")
        print("Done")
        return 0


if __name__ == "__main__":
    sys.exit(main())
