#!/usr/bin/env python
from __future__ import (division, print_function)

import time
from memory_profiler import profile


class DataCache(object):
    _duration = 300  # 5 minitues
    _lastedActiveDataFromAgent = {}

    @staticmethod
    def setDuration(sec):
        DataCache._duration = sec

    @staticmethod
    def getlatestJobData():
        if (DataCache._lastedActiveDataFromAgent):
            return DataCache._lastedActiveDataFromAgent["data"]
        else:
            return {}

    @staticmethod
    def setlatestJobData(jobData):
        DataCache._lastedActiveDataFromAgent["time"] = int(time.time())
        DataCache._lastedActiveDataFromAgent["data"] = jobData

    @staticmethod
    def islatestJobDataExpired():
        if not DataCache._lastedActiveDataFromAgent:
            return True

        if (int(time.time()) - DataCache._lastedActiveDataFromAgent["time"]) > DataCache._duration:
            return True
        return False

    @staticmethod
    def summary():
        print("DataCache type: {}, DataCache id: {}, DataCache.lastedActiveDataFromAgent id: {}".format(type(DataCache),
                                                                                                        id(DataCache),
                                                                                                        id(DataCache._lastedActiveDataFromAgent)))

@profile
def gatherActiveDataStats(inputData):
    if DataCache.islatestJobDataExpired():
        thisDict = {inputData: inputData * 5, "000": "blah"}
        DataCache.setlatestJobData(thisDict)
        print("DataCache is updated: {}".format(len(thisDict)))
    else:
        print("DataCache is up-to-date")
    DataCache.summary()


if __name__ == "__main__":
    DataCache.setDuration(15)
    for i in range(5):
        gatherActiveDataStats(i)
        time.sleep(10)
