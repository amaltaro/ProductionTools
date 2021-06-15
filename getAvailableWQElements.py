"""
Mimics the list function to fetch GQEs for a given team and set of resources
@author: amaltaro
"""

import sys
import os
import json
from pprint import pprint

from WMCore.Configuration import loadConfigurationFile
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend




def main():
    """
    Whatever
    """
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    options = {}
    options['include_docs'] = True
    options['descending'] = True
    options['num_elem'] = 10
    options['skip'] = 0
    options['limit'] = 50
    options['resources'] = {"T3_US_NERSC": 1000.0}
    options['team'] = "hepcloud"

    globalWQBackend = WorkQueueBackend(config.WorkloadSummary.couchurl, db_name="workqueue")
    for numSkip in range(0, 1500 + 1, 50):
        options['skip'] = numSkip
        print("Fetching GQEs from {} with options: {}".format(globalWQBackend.server.url, options))
        data = globalWQBackend.db.loadList('WorkQueue', 'workRestrictions', 'availableByPriority', options)
        data = json.loads(data)
        for item in data:
            wfName = item["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"]['RequestName']
            prio = item["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"]['Priority']
            print("{} with prio {} and id: {}".format(wfName, prio, item['_id']))

    #pprint(data)

    sys.exit(0)

if __name__ == "__main__":
    sys.exit(main())
