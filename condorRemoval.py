#!/usr/bin/env python
"""
Removes jobs that are pending in condor for more than 4 days.
"""
import sys
import os
import time
import classad
import htcondor as condor
from pprint import pprint


def main():
    # Retrieve all jobs from condor schedd
    schedd = condor.Schedd()
    jobs = schedd.xquery('true', ['WMAgent_RequestName', 'JobStatus', 'WMAgent_JobID', 'ServerTime', 'EnteredCurrentStatus'])

    threshold = 4 * 24 * 3600  # 4 days
    listJobsToRemove = []
    for job in jobs:
        if job['JobStatus'] == 1:
            timeThisStatus = job['ServerTime'] - job['EnteredCurrentStatus']
            if timeThisStatus > threshold:
                listJobsToRemove.append(job['WMAgent_JobID'])

    print "List of WMBS IDs to remove from condor: %s" % len(listJobsToRemove)
    sys.exit(1)

    ad = classad.ClassAd()
    ad['foo'] = listJobsToRemove
    jobsConstraint = "member(WMAgent_JobID, %s)" % ad.lookup("foo").__repr__()
    out = schedd.act(condor.JobAction.Remove, jobsConstraint)

    print "Done!"
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
