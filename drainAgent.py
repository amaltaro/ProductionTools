#!/usr/bin/env python
"""
Gets a list of workflows and lfns (taken from an agent that is about to be
redeployed) and:
 1. make a list of datasets out of the lfns passed in
 2. check if those datasets belong to any of the workflows passed in (known
 by the agent)
 3. print whether it's an input or output dataset of a know workflow
 """
import sys
import os
import httplib
import json


def lfn2dset(lfns):
    """ Convert a LFN into a dataset name """
    if isinstance(lfns, basestring):
        lfns = [lfns]

    listDsets = set()
    for lfn in set(lfns):
        toks = lfn.split('/')[3:]
        toks.pop(5)
        toks.pop(4)

        toks.insert(1, toks.pop(0))
        toks[1] += '-' + toks.pop(3)
        toks.insert(0, '')
        listDsets.add("/".join(toks))

    return listDsets


def getRequestDict(wfs):
    """ Retrieve the request workload schema from reqmgr2 """
    if isinstance(wfs, basestring):
        wfs = [wfs]

    url = 'cmsweb.cern.ch'
    head = {"Content-type": "application/json", "Accept": "application/json"}

    schema = {}
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    for wf in set(wfs):
        urn = "/reqmgr2/data/request/%s" % wf
        conn.request("GET", urn, headers=head)
        resp = conn.getresponse()
        data = resp.read()
        request = json.loads(data)["result"][0][wf]
        schema[wf] = filterKeys(request)

    return schema


def filterKeys(schema):
    """ Saves only input and output dataset info """
    newSchema = {}
    newSchema['RequestStatus'] = schema.get('RequestStatus', "")
    newSchema['InputDataset'] = schema.get('InputDataset', "")
    newSchema['OutputDatasets'] = schema.get('OutputDatasets', "")

    if schema['RequestType'] in ['StepChain', 'TaskChain']:
        joker = schema['RequestType'].split('Chain')[0]
        numInnerDicts = schema[schema['RequestType']]
        # and now we look for this key in each Task/Step
        for i in range(1, numInnerDicts + 1):
            innerName = "%s%s" % (joker, i)
            if 'InputDataset' in schema[innerName]:
                newSchema['InputDataset'] = schema[innerName]['InputDataset']
    return newSchema


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python drainAgent.py LIST_OF_WFS LIST_OF_LFNs")
        sys.exit(0)

    wfsFile = sys.argv[1]
    with open(wfsFile) as f:
        wfsList = [line.rstrip('\n') for line in f]

    lfnsFile = sys.argv[2]
    with open(lfnsFile) as f:
        lfnsList = [line.rstrip('\n') for line in f]

    dsetsList = lfn2dset(lfnsList)
    requestsDict = getRequestDict(wfsList)

    # now find these bastards
    for dset in dsetsList:
        print("%-150s is ..." % dset),
        for wf, data in requestsDict.iteritems():
            if dset == data['InputDataset']:
                print(" INPUT for %s which is in %s status" % (wf, data['RequestStatus'])),
            if dset in data['OutputDatasets']:
                print(" OUTPUT for %s which is in %s status" % (wf, data['RequestStatus'])),
        print("")

    sys.exit(0)
