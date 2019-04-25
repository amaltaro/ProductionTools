#!/usr/bin/env python
"""
You need to have your proxy created and in the environment;
or run it from an agent.
"""
from __future__ import print_function

import sys

from dbs.apis.dbsClient import DbsApi


def main():
    if len(sys.argv) != 2:
        print("You must provide a dataset name. E.g.: python listDsetLumis.py /EGamma/Run2018A-v1/RAW")
        sys.exit(1)
    dset = sys.argv[1]

    dbsApi = DbsApi(url='https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/')
    listBlocks = dbsApi.listBlocks(dataset=dset)
    for block in listBlocks:
        print("\nBlock: %s" % block['block_name'])
        blockInfo = dbsApi.listFileLumis(block_name=block['block_name'])
        for info in blockInfo:
            print("LFN: %s" % info['logical_file_name'])
            print("    Total lumis: %s\tLumis: %s" % (len(info['lumi_section_num']),
                                                      sorted(info['lumi_section_num'])))

if __name__ == "__main__":
    sys.exit(main())
