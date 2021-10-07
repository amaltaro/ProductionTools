#!/usr/bin/env python
import sys
import os
import json
import httplib

def getRequestsByStatus(status):
    url = 'cmsweb.cern.ch'
    headers = {"Accept": "application/json", "Content-type": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request?status=%s&mask=ScramArch" % status
    conn.request("GET", urn, headers=headers)
    resp = conn.getresponse()
    if resp.status != 200:
        print "Response status: %s\tResponse reason: %s" % (resp.status, resp.reason)
        print "Error message: %s" % resp.msg.getheader('X-Error-Detail')
        sys.exit(1)
    data = resp.read()
    data = json.loads(data)['result']
    if data:
        data = data[0]
    return data


def main():
    totalWorkflows = 0
    slc6Workflows = 0
    allStatus = ["assigned", "staging", "staged", "acquired",
                 "running-open", "running-closed",
                 "completed", "closed-out", "announced"]
    for status in allStatus:
        data = getRequestsByStatus(status)
        print("Retrieved {} requests in status: {}".format(len(data), status))
        if not data:
            continue

        ### Now parses the output and print SLC6 requests
        for req, reqDict in data.items():
            totalWorkflows += 1
            for scram in reqDict['ScramArch']:
                if scram.startswith("slc5_"):
                    print("Request: {:<150} using ScramArch: {}".format(req, reqDict['ScramArch']))
                    slc6Workflows += 1
        print("")
    print("\nSummary: there are {} SLC5 workflows out of a total {} active workflows".format(slc6Workflows, totalWorkflows))
    print("Done!")
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
