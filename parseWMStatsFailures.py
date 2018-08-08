#!/usr/bin/env python
"""
Fetch data from wmstats - for a specific workflow - and print a few tasks and the number
of job failures for CERN sites (read known sites that stage to T2_CH_CERN).
"""
import sys
import os
import json
import httplib
from pprint import pformat


def getWMStatsData(workflow):
    url = 'cmsweb.cern.ch'
    headers = {"Accept": "application/json", "Content-type": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    urn = "/wmstatsserver/data/request/%s" % workflow
    conn.request("GET", urn, headers=headers)
    r2=conn.getresponse()
    request = json.loads(r2.read())["result"][0]

    return request


def main():
    data = getWMStatsData("pdmvserv_task_HIG-RunIIFall17wmLHEGS-01116__v1_T_180418_043017_9486")
    data = data['pdmvserv_task_HIG-RunIIFall17wmLHEGS-01116__v1_T_180418_043017_9486']['AgentJobInfo']

    for agent in data.keys():
        print("\nStats for agent: %s" % agent)
        print("  Skipped files: %s" % pformat(data[agent]['skipped']))
        print("  Overall agent status: %s" % pformat(data[agent]['status']))
        task1 = data[agent]['tasks']['/pdmvserv_task_HIG-RunIIFall17wmLHEGS-01116__v1_T_180418_043017_9486/HIG-RunIIFall17wmLHEGS-01116_0']['sites']
        summary = {k: v.get('failure') for k, v in task1.items() if "CERN" in k}
        print("  Task1 failures at CERN: %s" % summary)
        task1 = data[agent]['tasks']['/pdmvserv_task_HIG-RunIIFall17wmLHEGS-01116__v1_T_180418_043017_9486/HIG-RunIIFall17wmLHEGS-01116_0/HIG-RunIIFall17wmLHEGS-01116_0CleanupUnmergedLHEoutput']['sites']
        summary = {k: v.get('failure') for k, v in task1.items() if "CERN" in k}
        print("  Task1 Cleanup LHE failures at CERN: %s" % summary)
        task1 = data[agent]['tasks']['/pdmvserv_task_HIG-RunIIFall17wmLHEGS-01116__v1_T_180418_043017_9486/HIG-RunIIFall17wmLHEGS-01116_0/HIG-RunIIFall17wmLHEGS-01116_0CleanupUnmergedRAWSIMoutput']['sites']
        summary = {k: v.get('failure') for k, v in task1.items() if "CERN" in k}
        print("  Task1 Cleanup RAWSIM failures at CERN: %s" % summary)

        task2 = data[agent]['tasks']['/pdmvserv_task_HIG-RunIIFall17wmLHEGS-01116__v1_T_180418_043017_9486/HIG-RunIIFall17wmLHEGS-01116_0/HIG-RunIIFall17DRPremix-00782_0']['sites']
        summary = {k: v.get('failure') for k, v in task2.items() if "CERN" in k}
        print("  Task2 failures at CERN: %s" % summary)
        task2 = data[agent]['tasks']['/pdmvserv_task_HIG-RunIIFall17wmLHEGS-01116__v1_T_180418_043017_9486/HIG-RunIIFall17wmLHEGS-01116_0/HIG-RunIIFall17DRPremix-00782_0/HIG-RunIIFall17DRPremix-00782_0CleanupUnmergedPREMIXRAWoutput']['sites']
        summary = {k: v.get('failure') for k, v in task2.items() if "CERN" in k}
        print("  Task2 Cleanup PREMIXRAW failures at CERN: %s" % summary)

        task3 = data[agent]['tasks']['/pdmvserv_task_HIG-RunIIFall17wmLHEGS-01116__v1_T_180418_043017_9486/HIG-RunIIFall17wmLHEGS-01116_0/HIG-RunIIFall17DRPremix-00782_0/HIG-RunIIFall17DRPremix-00782_1']['sites']
        summary = {k: v.get('failure') for k, v in task3.items() if "CERN" in k}
        print("  Task3 failures at CERN: %s" % summary)

    print "Done!"
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
