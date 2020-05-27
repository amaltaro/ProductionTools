#!/usr/bin/env python
from __future__ import (division, print_function)

import time
import os
import sys
import traceback
from memory_profiler import profile
from WMCore.WMStats.DataStructs.DataCache import DataCache
from WMCore.Services.WMStats.WMStatsReader import WMStatsReader

@profile
def gatherActiveDataStats():
    wmstats_url = "https://cmsweb.cern.ch/couchdb/wmstats"
    reqmgrdb_url = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
    jobInfoFlag = False
    tStart = time.time()
    try:
        if DataCache.islatestJobDataExpired():
            wmstatsDB = WMStatsReader(wmstats_url, reqdbURL=reqmgrdb_url, reqdbCouchApp="ReqMgr")
            jobData = wmstatsDB.getActiveData(jobInfoFlag=jobInfoFlag)
            DataCache.setlatestJobData(jobData)
            print("DataCache is updated: {}".format(len(jobData)))
        else:
            print("DataCache is up-to-date")
    except Exception as ex:
        print("Exception updating cache. Details: {}\nTraceback: {}".format(str(ex), str(traceback.format_exc())))
    print("Total time executing this cycle: {}".format(time.time() - tStart))


if __name__ == "__main__":
    DataCache.setDuration(100)
    while True:
        gatherActiveDataStats()
        time.sleep(60)
