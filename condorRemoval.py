#!/usr/bin/env python
"""
Removes jobs that are in the same condor status for more than 4 days.
"""
import sys
import os
import time
import json
import classad
import htcondor as condor
from pprint import pprint


def main():
    # Retrieve all jobs from condor schedd
    schedd = condor.Schedd()
    jobs = schedd.xquery('true', ['WMAgent_RequestName', 'JobStatus', 'WMAgent_JobID', 'ServerTime', 'EnteredCurrentStatus'])

    threshold = 4 * 24 * 3600  # 4 days
    listJobsToRemove = []
    jobsRemovedInfo = []
    for job in jobs:
        timeThisStatus = job['ServerTime'] - job['EnteredCurrentStatus']
        if timeThisStatus > threshold:
            listJobsToRemove.append(job['WMAgent_JobID'])
            jobsRemovedInfo.append(job)

    if jobsRemovedInfo:
        with open('jobs_removed_script.txt', 'w') as f:
            for line in jobsRemovedInfo:
                f.writelines(str(line))

    print "Number of jobs to be removed from condor: %s" % len(listJobsToRemove)

    ad = classad.ClassAd()
    ad['foo'] = listJobsToRemove
    jobsConstraint = "member(WMAgent_JobID, %s)" % ad.lookup("foo").__repr__()
    out = schedd.act(condor.JobAction.Remove, jobsConstraint)
    print "Outcome: %s" % out
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
