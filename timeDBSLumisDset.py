import sys
import timeit

from pprint import pprint
from WMCore import Lexicon
from WMCore.DataStructs.LumiList import LumiList
from datetime import datetime

def test(num_lumis):
    from dbs.apis.dbsClient import DbsApi
    dbsApi = DbsApi(url = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/')
    #dbsApi = DbsApi(url = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/')
    datasetPath = "/SMS-T5qqqqVV_mGluino-1200To1275_mLSP-1to1150_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIIWinter15pLHE-MCRUN2_71_V1-v1/LHE"
    #datasetPath = "/QDTojWinc_NC_M-1200_TuneZ2star_8TeV-madgraph/Summer12pLHE-DMWM_Validation_DONOTDELETE_Alan_TEST-v1/GEN"
    run = 1
    lumis = range(1, num_lumis+1)

    files = []
    print "Starting queries to listFileArray (in dataset mode) at %s" % datetime.utcnow()
    for slumis in Lexicon.slicedIterator(lumis, 10):
        start = datetime.utcnow()
        print slumis
        slicedFiles = dbsApi.listFileArray(dataset=datasetPath, run_num=run, lumi_list=slumis, detail=True)
        files.extend(slicedFiles)
        end = datetime.utcnow()
        print "  slice completed in %s" % (end - start)

#    pprint(files)
    maskedBlocks = {}
    for lfn in files:
        blockName = lfn['block_name']
        fileName = lfn['logical_file_name']
        if blockName not in maskedBlocks:
            maskedBlocks[blockName] = {}
        if fileName not in maskedBlocks[blockName]:
            maskedBlocks[blockName][fileName] = LumiList()

#    pprint(maskedBlocks)
    print "\nStarting queries to listFileLumis at %s" % datetime.utcnow()
    for block in maskedBlocks:
        print block
        start = datetime.utcnow()
        fileLumis = dbsApi.listFileLumis(block_name=block, validFileOnly = 1)
        end = datetime.utcnow()
        print "  query completed in %s" % (end - start)


def main():
    print "Script started at %s" % datetime.utcnow()
    numLumis = sys.argv[1] if len(sys.argv) == 2 else 1
    ti = timeit.timeit("test(%d)" % int(numLumis), setup="from __main__ import test", number=1)
    print "Lumis: %s\tSeconds: %s" % (numLumis, ti)

if __name__ == "__main__":
    sys.exit(main())
