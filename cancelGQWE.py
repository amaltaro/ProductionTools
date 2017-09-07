"""
_cancelGQWE_

This script mimics exactly the same action as of a workflow
being aborted (marking all global workqueue elements as
CancelRequested).

The only thing left is the final reqmgr state transition, so
one has to move the workflow from XXX to aborted at the end.
"""
from __future__ import print_function

import sys
from Utils.IteratorTools import grouper
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

def main():
    # FIXME update the workflow name here
    wf = "mcremone_task_EXO-RunIISummer15wmLHEGS-04802__v1_T_170811_181808_305"
    print("Looking for problematic inbox elements...")

    wq = WorkQueue("https://cmsweb.cern.ch/couchdb/workqueue")
    print("Workqueue config: server %s and db %s" % (wq.server.url, wq.db.name))

    nonCancelableElements = ['Done', 'Canceled', 'Failed']
    data = wq.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                          {'startkey': [wf], 'endkey': [wf, {}], 'reduce': False})

    elements = [x['id'] for x in data.get('rows', []) if x['key'][1] not in nonCancelableElements]
    print("Found %d elements for wf %s" % (len(elements), wf))
    total = 0
    for eleSlice in grouper(elements, 100):
        try:
            wq.updateElements(*eleSlice, Status='CancelRequested')
        except Exception as ex:
            print("Exception happened, but keep going: %s" % str(ex))
        else:
            total += 100
            print("Elements updated: %s" % total)

    print("Done!")

    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())
