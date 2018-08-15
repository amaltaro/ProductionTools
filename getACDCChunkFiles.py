from __future__ import print_function
import json
import pickle
import time
from pprint import pprint, pformat
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.WorkQueue.DataStructs.ACDCBlock import ACDCBlock
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


def main():
    start = time.time()
    # blockName = match['Inputs'].keys()[0]
    blockName = "/acdc/vlimant_ACDC0_task_HIG-RunIIFall17wmLHEGS-01122__v1_T_180808_130708_5376/:pdmvserv_task_HIG-RunIIFall17wmLHEGS-01122__v1_T_180415_203643_8440:HIG-RunIIFall17wmLHEGS-01122_0:HIG-RunIIFall17DRPremix-00788_0/0/102323"

    # acdcInfo = match['ACDC']
    acdcInfo = {"database": "acdcserver",
                "fileset": "/pdmvserv_task_HIG-RunIIFall17wmLHEGS-01122__v1_T_180415_203643_8440/HIG-RunIIFall17wmLHEGS-01122_0/HIG-RunIIFall17DRPremix-00788_0",
                "collection": "pdmvserv_task_HIG-RunIIFall17wmLHEGS-01122__v1_T_180415_203643_8440",
                "server": "https://cmsweb.cern.ch/couchdb"}

    acdc = DataCollectionService(acdcInfo["server"], acdcInfo["database"])
    splitedBlockName = ACDCBlock.splitBlockName(blockName)
    print("Splitted block name: %s" % splitedBlockName)

    fileLists = acdc.getChunkFiles(acdcInfo['collection'],
                                   acdcInfo['fileset'],
                                   splitedBlockName['Offset'],
                                   splitedBlockName['NumOfFiles'])
    print("Retrieved %d unique files from the ACDCServer" % len(fileLists))

    block = {}
    block["Files"] = fileLists

    # with open("chunkfiles.json", 'w') as fo:
    #     json.dump(block, fo)
    with open("chunkfiles.pkl", 'w') as fo:
        pickle.dump(block, fo)

    end = time.time()
    print("Spent %s secs running so far" % (end - start))

    ### Now doing the WMBSHelper stuff
    reqUrl = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
    requestName = "vlimant_ACDC0_task_HIG-RunIIFall17wmLHEGS-01122__v1_T_180808_130708_5376"

    wmspec = WMWorkloadHelper()
    wmspec.loadSpecFromCouch(reqUrl, requestName)
    taskName = "HIG-RunIIFall17DRPremix-00788_0"
    mask = None
    cacheDir = "/data/srv/wmagent/v1.1.14.patch6/install/wmagent/WorkQueueManager/cache"
    # wmbsHelper = WMBSHelper(wmspec, match['TaskName'], blockName, mask, self.params['CacheDir'])
    wmbsHelper = WMBSHelper(wmspec, taskName, blockName, mask, cacheDir)
    sub, numFilesAdded = wmbsHelper.createSubscriptionAndAddFiles(block=block)

if __name__ == '__main__':
    main()