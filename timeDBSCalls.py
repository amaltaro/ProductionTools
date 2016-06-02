import sys
import timeit
from datetime import datetime
from WMCore.WorkQueue.WorkQueueUtils import get_dbs


def test(dbsUrl):
    # super big dataset, 540 blocks and 10775 files
    #datasetPath = '/SingleElectron/Run2012D-v1/RAW'
    # smaller, only 14 blocks and 2954 files
    datasetPath = '/MinBias_TuneZ2star_8TeV-pythia6/Summer12-START50_V13-v3/GEN-SIM'

    dbs = get_dbs(dbsUrl)
    blocks = []
    for block in dbs.listFileBlocks(datasetPath, onlyClosedBlocks=True):
        blocks.append(str(block))

    for blockName in blocks:
        dbs.getDBSSummaryInfo(datasetPath, block=blockName)
        runLumis = dbs.listRunLumis(block=blockName)
        fileInfo = dbs.listFilesInBlock(fileBlockName=blockName)

def main():
    dbsUrl = 'https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/'
    print "Started evaluation of %s at: %s" % (dbsUrl, datetime.utcnow())
    ti = timeit.timeit("test('%s')" % dbsUrl, setup="from __main__ import test", number=1)
    print "Finished in %s secs" % ti

    dbsUrl = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader/'
    print "Started evaluation of %s at: %s" % (dbsUrl, datetime.utcnow())
    ti = timeit.timeit("test('%s')" % dbsUrl, setup="from __main__ import test", number=1)
    print "Finished in %s secs" % ti


if __name__ == "__main__":
    sys.exit(main())
