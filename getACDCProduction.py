from __future__ import print_function
import json
import pickle
import time
import sys
from pprint import pprint, pformat
from WMCore.ACDC.DataCollectionService import DataCollectionService, mergeFilesInfo
from WMCore.WorkQueue.DataStructs.ACDCBlock import ACDCBlock
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


def main():
    start = time.time()

    # acdcInfo = match['ACDC']
    acdcInfo = {"database": "acdcserver",
                "fileset": "/pdmvserv_task_SUS-RunIIFall18wmLHEGS-00025__v1_T_181211_005112_2222/SUS-RunIIFall18wmLHEGS-00025_0",
                "collection": "pdmvserv_task_SUS-RunIIFall18wmLHEGS-00025__v1_T_181211_005112_2222",
                "server": "https://cmsweb.cern.ch/couchdb"}

    dcs = DataCollectionService(acdcInfo["server"], acdcInfo["database"])
#    acdcFileList = dcs.getProductionACDCInfo(acdcInfo['collection'], acdcInfo['fileset'])

    files = dcs._getFilesetInfo(acdcInfo['collection'], acdcInfo['fileset'])
    print("%s" % pformat(files[0]))
    files = mergeFilesInfo(files)
    acdcFileList = []
    for value in files:
        fileInfo = {"lfn": value["lfn"],
                    "first_event": value["first_event"],
                    "lumis": value["runs"][0]["lumis"],
                    "events": value["events"]}
        acdcFileList.append(fileInfo)
    #print("Data retrieved:\n%s" % pformat(acdcFileList))
    print("Retrieved %d files from the ACDCServer" % len(acdcFileList))

    listLumis = []
    wantedLumis = set([252052, 240646])
    for f in acdcFileList:
        listLumis.extend(f['lumis'])
        lumisSet = set(f['lumis'])
        if wantedLumis.intersection(lumisSet):
            print("File: %s with events: %s, contains these lumis: %s" % (f['lfn'], f['events'], f['lumis']))

    print("Total amount of lumis: %d, where unique are: %d" % (len(listLumis), len(set(listLumis))))
    # with open("chunkfiles.json", 'w') as fo:
    #     json.dump(block, fo)

    end = time.time()
    print("Spent %s secs running so far" % (end - start))
    sys.exit(1)


if __name__ == '__main__':
    main()
