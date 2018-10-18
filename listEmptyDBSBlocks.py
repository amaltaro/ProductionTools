#!/usr/bin/env python
"""
You need to have your proxy created and in the environment
"""
from __future__ import print_function

import sys

from dbs.apis.dbsClient import DbsApi


def updateSummary(summary, blockInfo):
    """
    [{'num_file': 500, 'file_size': 4617905583461, 'num_event': 5443067, 'num_lumi': 622, 'num_block': 1}]
    """
    summary.setdefault('num_file', 0)
    summary.setdefault('num_block', 0)
    summary.setdefault('num_event', 0)
    summary.setdefault('num_lumi', 0)

    summary['num_file'] += blockInfo[0]['num_file']
    summary['num_block'] += blockInfo[0]['num_block']
    summary['num_event'] += blockInfo[0]['num_event']
    summary['num_lumi'] += blockInfo[0]['num_lumi']


def main():
    if len(sys.argv) != 2:
        print("You must provide a dataset name. E.g.: python listEmptyDBSBlocks.py /EGamma/Run2018A-v1/RAW")
        sys.exit(1)
    dset = sys.argv[1]

    summaryLoss = {}
    badBlocks = []
    dbsApi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader/')
    listBlocks = dbsApi.listBlocks(dataset=dset)
    for block in listBlocks:
        blockInfo = dbsApi.listFileSummaries(block_name=block['block_name'], validFileOnly=1)
        if not blockInfo or not blockInfo[0]['num_file']:
            blockInfo = dbsApi.listFileSummaries(block_name=block['block_name'], validFileOnly=0)
            print("Block %s doesn't contain any valid files. Block summary: %s" % (block['block_name'], blockInfo))
            updateSummary(summaryLoss, blockInfo)
            badBlocks.append(block['block_name'])
    print("List of blocks that will be skipped:\n%s\n" % list(set(badBlocks)))
    print("Summary of blocks with all files invalid: %s" % summaryLoss)


if __name__ == "__main__":
    sys.exit(main())
