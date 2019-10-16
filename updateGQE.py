"""
_updateGQE_

Can be converted to a generic script that updates global workqueue
elements, provided some filters and which property to be updated.

For this specific case, it looks for StoreResults elements sitting
in the Available status. It then queries DBS phys03 instance, fetches
the input data location and updates the GQE with the new location.

Run it from the agent, with the agent environment loaded
"""
from __future__ import print_function

import sys

from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend

backend = WorkQueueBackend('https://cmsweb.cern.ch/couchdb')


def isDataset(inputData):
    """Check whether we're handling a block or a dataset"""
    if '#' in inputData.split('/')[-1]:
        return False
    return True


def getProblematicRequests():
    """
    _getProblematicRequests_
    """
    elements = backend.getElements(status="Available", TaskName="StoreResults")
    print("Found %d StoreResults GQE elements in Available status" % len(elements))
    return elements


def main():
    problemElems = getProblematicRequests()
    print("Found %d bad elements that needs fixup" % len(problemElems))
    if not problemElems:
        print("Nothing to fix, contact a developer if the problem persists...")
        return 0

    cric = CRIC()
    dbsUrl = "https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader"
    dbs = DBSReader(dbsUrl)

    for elem in problemElems:
        print("Handling id: %s, with inputs: %s" % (elem.id, elem['Inputs']))
        for dataItem in elem['Inputs']:
            if isDataset(dataItem):
                pnns = dbs.listDatasetLocation(dataItem, dbsOnly=True)
            else:
                pnns = dbs.listFileBlockLocation(dataItem, dbsOnly=True)
            psns = cric.PNNstoPSNs(pnns)
            print("  PNNs: %s map to PSNs: %s" % (pnns, psns))
            elem['Inputs'][dataItem] = psns

    backend.saveElements(*problemElems)
    print("Done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
