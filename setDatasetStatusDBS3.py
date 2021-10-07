#!/usr/bin/env python3
"""
_setDatasetStatus_

Given a dataset name and status, it sets the DBS status of this dataset to the status provided.

"""
import sys
import os
from dbs.apis.dbsClient import DbsApi
from optparse import OptionParser


def setStatusDBS3(url3, dataset3, newStatus, files):
    dbsapi = DbsApi(url=url3)
    dbsapi.updateDatasetType(dataset=dataset3, dataset_access_type=newStatus)

    ### invalidating the files
    if files:
        if newStatus in ['DELETED', 'DEPRECATED', 'INVALID']:
            file_status = 0
        elif newStatus in ['PRODUCTION', 'VALID']:
            file_status = 1
        else:
            print("Sorry, I don't know this state and you cannot set files to %s" % newStatus)
            print("Only the dataset was changed. Quitting the program!")
            sys.exit(1)
        print("Files will be set to: {} in DBS3".format(file_status))
        files = dbsapi.listFiles(dataset=dataset3)
        for this_file in files:
            dbsapi.updateFileStatus(logical_file_name=this_file['logical_file_name'],is_file_valid=file_status)


def main():

    usage="usage: python setDatasetStatus.py --dataset=<DATASET_NAME> --status=<STATUS> {--files}"
    parser = OptionParser(usage=usage)
    parser.add_option('-d', '--dataset', dest='dataset', default=None, help='Dataset name')
    parser.add_option('-s', '--status', dest='status', default=None, help='This will be the new status of the dataset/files')
    parser.add_option('-f', '--files', action="store_true", default=False, dest='files', help='Validate or invalidate all files in dataset')

    (opts, args) = parser.parse_args()

    command=""
    for arg in sys.argv:
        command=command+arg+" "

    if opts.dataset == None:
        print("--dataset option must be provided")
        print(usage)
        sys.exit(1)
    if opts.status == None:
        print("--status option must be provided")
        print(usage)
        sys.exit(1)

    setStatusDBS3('https://cmsweb-testbed.cern.ch/dbs/int/global/DBSWriter', opts.dataset, opts.status, opts.files)

    print("Done")
    sys.exit(0)

if __name__ == "__main__":
    main()
