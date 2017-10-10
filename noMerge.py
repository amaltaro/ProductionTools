"""
Script used to find out what are the workflows still in the system that might be
affected by #7998, where the agent is NOT backwards compatible and merge tasks/jobs
don't get created.

It queries several services:
 1. given a date range (actually a month), it retrieved all workflows created in that range
 2. if the workflow is active, then it tries to find the agents that were working on that request
in the end it prints a summary of the amount and request names that might be affected.

One still has to do a last check and feed that list of workflows into updateSpec.py, such that
the spec files get properly updated and can then be processed by the new agent.
"""
from __future__ import print_function

import httplib
import json
import os
import sys
from collections import defaultdict
from time import strptime

url = "cmsweb.cern.ch"


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
        if status in ("new", "assignment-approved", "assigned", "acquired"):
            printNonAgent(wfList)  # hasn't been pulled yet
        else:
            res = getAgentName(wfList)

    sys.exit(0)


def printNonAgent(requests):
    for req in requests:
        print("    %-105s : []" % req)
    return


def retrieveByDate(month):
    """Given a month name, fetch all workflows and their status from couchdb"""
    monthNum = strptime(month, "%b").tm_mon
    startdate = [2017, monthNum, 1]
    if month == 'Sep':
        enddate = [2017, monthNum, 13]
    else:
        enddate = [2017, monthNum + 1, 1]

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
        print("    %-105s : %s" % (req, agentList))
        results[req] = agentList

    return results


if __name__ == '__main__':
    main()
