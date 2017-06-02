"""
Reads in a TaskChain request and convert it to a StepChain one

Pre-requisites:
 1. a valid proxy in your X509_USER_PROXY variable
"""

import sys
import os
import json
import httplib
from copy import copy
from pprint import pprint

url = "cmsweb-testbed.cern.ch"
reqmgrCouchURL = "https://" + url + "/couchdb/reqmgr_workload_cache"

DEFAULT_DICT = {
    "AcquisitionEra": "UPDATEME",
    "CMSSWVersion": "UPDATEME",
    "Campaign": "UPDATEME",
    "ConfigCacheUrl": "https://cmsweb.cern.ch/couchdb",
    "DQMConfigCacheID": "UPDATEME",
    "DQMHarvestUnit": "UPDATEME",
    "DQMUploadUrl": "UPDATEME",
    "DbsUrl": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/",
    "EnableHarvesting": "UPDATEME",  # boolean
    "GlobalTag": "UPDATEME",
    "Memory": "UPDATEME",  # integer
    "Multicore": "UPDATEME", # integer
    "PrepID": "UPDATEME",
    "PrimaryDataset": "UPDATEME",
    "ProcessingString": "UPDATEME",
    "ProcessingVersion": 1,
    "RequestPriority": "UPDATEME",  # integer
    "RequestString": "UPDATEME",
    "RequestType": "StepChain",
    "ScramArch": "UPDATEME",
    "SizePerEvent": "UPDATEME",  # integer
    "StepChain": "UPDATEME",  # integer
    "TimePerEvent": "UPDATEME"}



def main():
    if len(sys.argv) != 2:
        print "Usage: python taskChain2StepChain.py WORKFLOW_NAME"
        sys.exit(0)

    work = retrieveWorkload(sys.argv[1])
    newDict = buildRequest(work)
    print("Creating StepChain workflow for: %s" % sys.argv[1])
    workflow = submitWorkflow(newDict)
    approveRequest(workflow)
    sys.exit(0)


def retrieveWorkload(workflowName):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflowName
    conn.request("GET", urn, headers=headers)
    r2 = conn.getresponse()
    request = json.loads(r2.read())["result"][0][workflowName]
    return request


def buildRequest(req_cache):
    newSchema = copy(DEFAULT_DICT)
    # first update top level dict
    for k, v in DEFAULT_DICT.iteritems():
        if v != "UPDATEME":
            continue
        if k == 'RequestString':
            newSchema[k] = req_cache[k] + '_Converted'
        elif k == "PrepID":
            newSchema[k] = 'TEST-' + req_cache[k]
        elif k == "StepChain":
            newSchema[k] = req_cache["TaskChain"]
        elif k in req_cache:
            newSchema[k] = req_cache[k]

    # then build the steps
    timePerEvent = 0
    sizePerEvent = 0
    memory = []
    for i in range(1, newSchema['StepChain'] + 1):
        newSchema['Step%s' % i] = req_cache['Task%s' % i]
        newSchema['Step%s' % i]['StepName'] = newSchema['Step%s' % i].pop('TaskName')
        if 'InputTask' in newSchema['Step%s' % i]:
            newSchema['Step%s' % i]['InputStep'] = newSchema['Step%s' % i].pop('InputTask')

        timePerEvent += newSchema['Step%s' % i].pop('TimePerEvent', 0)
        sizePerEvent += newSchema['Step%s' % i].pop('SizePerEvent', 0)
        memory.append(newSchema['Step%s' % i].get('Memory', 0))
    # finally, override global value by sum of steps
    if timePerEvent > 0:
        newSchema['TimePerEvent'] = timePerEvent
    if sizePerEvent > 0:
        newSchema['SizePerEvent'] = sizePerEvent
    memory.append(newSchema.get('Memory', 0))
    newSchema['Memory'] = max(memory) 
    return newSchema


def submitWorkflow(schema):
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    encodedParams = json.dumps(schema)
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    #print "Submitting new workflow..."
    conn.request("POST", "/reqmgr2/data/request", encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print "Response status: %s\tResponse reason: %s" % (resp.status, resp.reason)
        print "Error message: %s" % resp.msg.getheader('X-Error-Detail')
        sys.exit(1)
    data = json.loads(data)
    requestName = data['result'][0]['request']
    print "  Request '%s' successfully created." % requestName
    return requestName


def approveRequest(workflow):
    #print "Approving request..."
    encodedParams = json.dumps({"RequestStatus": "assignment-approved"})
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}

    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    conn.request("PUT", "/reqmgr2/data/request/%s" % workflow, encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print "Response status: %s\tResponse reason: %s" % (resp.status, resp.reason)
        if hasattr(resp.msg, "x-error-detail"):
            print "Error message: %s" % resp.msg["x-error-detail"]
            sys.exit(2)
    conn.close()
    #print "  Request successfully approved!"
    return


if __name__ == '__main__':
    main()
