"""
__getGQByWorkflow.py__

Create a summary of the global workqueue docs by element status and agent handling work.

Created on July 15, 2016.
@author: amaltaro
"""

import sys
import os
from pprint import pprint

from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend


def createElementsSummary(reqName, elements, dbName):
    """
    Print the local couchdb situation based on the WQE status
    """
    print("Summary for request %s in the '%s' database" % (reqName, dbName))
    summary = {'numberOfElements': len(elements)}
    for elem in elements:
        summary.setdefault(elem['Status'], {})
        if elem['ChildQueueUrl'] not in summary[elem['Status']]:
            summary[elem['Status']][elem['ChildQueueUrl']] = 0
        summary[elem['Status']][elem['ChildQueueUrl']] += 1
    pprint(summary)


def main():
    """
    Whatever
    """
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    if len(sys.argv) != 2:
        print("You must provide a request name")
        sys.exit(1)

    reqName = sys.argv[1]

    globalWQBackend = WorkQueueBackend(config.WorkloadSummary.couchurl, db_name="workqueue")
    gqDocIDs = globalWQBackend.getElements(RequestName=reqName)
    createElementsSummary(reqName, gqDocIDs, 'workqueue')
    
    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())
