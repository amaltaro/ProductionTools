"""
Script used to find out what are the workflows still in the system that might be
affected by #7998, where the agent is NOT backwards compatible and merge tasks/jobs
don't get created.

It queries several services:
 1. given a date range (actually a month), it retrieved all workflows created in that range
 2. if the workflow is active, then it tries to find the agents that were working on that request
 3. TODO: if the workflow is affected, fetch its spec file from ReqMgr and update it
"""
from __future__ import print_function

import httplib
import json
import os
import sys
from collections import defaultdict
from copy import copy
from time import strptime

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

url = "cmsweb.cern.ch"
reqmgrCouchURL = "https://" + url + "/couchdb/reqmgr_workload_cache"
HELPER = WMWorkloadHelper()


def main():
    if len(sys.argv) != 2:
        print("Usage: python noMerge.py <month name with 3 letters only>")
        print(" e.g.: python noMerge.py Jul")
        sys.exit(0)

    month = sys.argv[1]
    wfs = retrieveByDate(month)
    print("Found a total of %d workflows for %s" % (len(wfs), month))
    archiveCount = 0  # number of archived workflows
    wfByStatus = defaultdict(list)
    for wf in wfs:
        wf = wf['value']
        if "archived" in wf['RequestStatus']:
            archiveCount += 1
        else:
            wfByStatus[wf['RequestStatus']].append(wf['RequestName'])

    for status, wfList in wfByStatus.iteritems():
        print("\nRetrieving agent info for %d requests in status %s" % (len(wfList), status))
        res = getAgentName(wfList)
    # pprint(dict(wfByStatus))
    # work = retrieveWorkload(sys.argv[1])
    # workflow = submitWorkflow(newDict)
    sys.exit(0)


def retrieveByDate(month):
    """Given a month name, fetch all workflows and their status from couchdb"""
    monthNum = strptime(month, "%b").tm_mon
    startdate = [2017, monthNum, 1]
    enddate = [2017, monthNum, 31]

    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    urn = "/couchdb/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=false&startkey=%s&endkey=%s" % (
    startdate, enddate)
    urn = urn.replace(" ", "")  # replace spaces for couch queries
    print("Querying couchdb for: %s" % urn)
    conn.request("GET", urn, headers=headers)
    r2 = conn.getresponse()
    request = json.loads(r2.read())["rows"]
    return request


def getAgentName(requestList):
    """
    Given a list of requests, return a dict with the agents that pulled work key'ed
    by the request name
    """
    headers = {"Content-type": "application/json",
               "Accept": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file=os.getenv('X509_USER_PROXY'), key_file=os.getenv('X509_USER_PROXY'))
    results = {}
    for req in requestList:
        urn = "/wmstatsserver/data/request/%s" % req
        # print("Querying wmstats for: %s" % urn)
        conn.request("GET", urn, headers=headers)
        r2 = conn.getresponse()
        reqDict = json.loads(r2.read())["result"][0][req]
        agentList = reqDict.get("AgentJobInfo", {}).keys()
        print("    %-150s : %s" % (req, agentList))
        results[req] = agentList

    return results


def updateSpec(requestName):
    """
    Given a request name, fetch its spec file from cmsweb couch and
    parse every single task and add one attribute to the input reference
    section
    """
    reqUrl = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
    HELPER.loadSpecFromCouch(reqUrl, requestName)


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
    conn.request("POST", "/reqmgr2/data/request", encodedParams, headers)
    resp = conn.getresponse()
    data = resp.read()
    if resp.status != 200:
        print("Response status: %s\tResponse reason: %s" % (resp.status, resp.reason))
        print("Error message: %s" % resp.msg.getheader('X-Error-Detail'))
        sys.exit(1)
    data = json.loads(data)
    requestName = data['result'][0]['request']
    print("  Request '%s' successfully created." % requestName)
    return requestName


if __name__ == '__main__':
    main()
