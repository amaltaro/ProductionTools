#!/usr/bin/env python
"""
You need to have your proxy created and in the environment
"""
from __future__ import print_function

import sys

from dbs.apis.dbsClient import DbsApi


def main():
    if len(sys.argv) != 2:
        print("You must provide a dataset name. E.g.: python listEmptyDBSBlocks.py.py /EGamma/Run2018A-v1/RAW")
        sys.exit(1)
    dset = sys.argv[1]

    dbsApi = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader/')
    listBlocks = dbsApi.listBlocks(dataset=dset)
    for block in listBlocks:
        blockInfo = dbsApi.listFileSummaries(block_name=block['block_name'], validFileOnly=1)
        if not blockInfo or not blockInfo[0]['num_file']:
            print("Block %s doesn't contain any valid files. Block summary: %s" % (block['block_name'], blockInfo))


if __name__ == "__main__":
    sys.exit(main())
