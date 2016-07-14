#!/usr/bin/env python
"""
Script used to clean some mess in the agents, on July 14. Problem in short,
jobs remain as active in BossAir db, in executing in WMBS and sometimes still
in condor.

Removes jobs from condor if they have exceeded the timeout for specific status.
Mark jobs in BossAir as completed and Timeout sched status.
"""
import sys
import os
import json
import threading
import logging
import time
import classad
import htcondor as condor
from pprint import pprint
from WMCore.WMInit import connectToDB
from WMCore.Database.DBFormatter import DBFormatter


getRunJobsActive = """
                   select id from bl_runjob where status=1 and status_time < :timestamp
                   """

updateState = """
              UPDATE bl_runjob SET status=0, sched_status=(select id from bl_status where name='Timeout')  WHERE id = :id
              """


def condorCleanup():
    # Retrieve all jobs from condor schedd
    schedd = condor.Schedd()
    jobs = schedd.xquery('true', ['WMAgent_RequestName', 'JobStatus', 'WMAgent_JobID', 'ServerTime', 'EnteredCurrentStatus'])

    # timeout keyed by condor status
    timeout = {1: 3.1 * 24 * 3600,  # Idle/Pending --> 3.1 days
               2: 2.1 * 24 * 3600,  # Running --> 2.1 days
               5: 0.1 * 24 * 3600}  # Held --> 0.1 days

    listJobsToRemove = []
    jobsRemovedInfo = []
    for job in jobs:
        if job['JobStatus'] not in (1, 2, 5):
            continue
        timeThisStatus = job['ServerTime'] - job['EnteredCurrentStatus']
        if timeThisStatus > timeout[job['JobStatus']]:
            listJobsToRemove.append(job['WMAgent_JobID'])
            jobsRemovedInfo.append(job)

    if jobsRemovedInfo:
        with open('jobs_removed_script.txt', 'w') as f:
            for line in jobsRemovedInfo:
                f.writelines(str(line))

    print "Number of jobs to be removed from condor: %s" % len(listJobsToRemove)

    ad = classad.ClassAd()
    while len(listJobsToRemove) > 0:
        ad['foo'] = listJobsToRemove[:100]
        listJobsToRemove = listJobsToRemove[100:]
        jobsConstraint = "member(WMAgent_JobID, %s)" % ad.lookup("foo").__repr__()
        out = schedd.act(condor.JobAction.Remove, jobsConstraint)
        #print "Outcome: %s" % str(out)
    return


def main():
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    if 'manage' not in os.environ:
        os.environ['manage'] = '/data/srv/wmagent/current/config/wmagent/manage'

    # first, break free from old condor jobs
    condorCleanup()

    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)

    time5d = int(time.time()) - 5 * 24 * 3600
    binds = [{'timestamp': time5d}]
    activeRunJobs = formatter.formatDict(myThread.dbi.processData(getRunJobsActive, binds))
    print "Found %d active jobs in BossAir older than 5 days" % len(activeRunJobs)

    # now mark these jobs as complete and in Timeout status
    binds = activeRunJobs[:10000]
    myThread.dbi.processData(updateState, binds)

    print "Done!"
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())


