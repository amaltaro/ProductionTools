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
import threading
import logging
from pprint import pprint

try:
    import htcondor as condor
    from WMCore.WMInit import connectToDB
    from WMCore.Database.DBFormatter import DBFormatter
    from WMCore.Configuration import loadConfigurationFile
    from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
    #from WMCore.DAOFactory import DAOFactory
except ImportError:
    print "You do not have a proper environment, please source the following:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(1)



jobCountByState = """
    select wmbs_job_state.name, count(*) AS count
    from wmbs_job
    join wmbs_job_state on (wmbs_job.state = wmbs_job_state.id)
    group by wmbs_job.state, wmbs_job_state.name"""

knownWorkflows = "SELECT DISTINCT name from wmbs_workflow"

workflowsNotInjected = "select distinct name from wmbs_workflow where injected = 0"


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


def getWorkflowStatus(config, listOfWfs):
    """
    Given a list of workflows, return their name and RequestStatus
    """
    if isinstance(listOfWfs, basestring):
        listOfWfs = [listOfWfs]

    wfDBReader = RequestDBReader(config.AnalyticsDataCollector.centralRequestDBURL,
                                 couchapp = config.AnalyticsDataCollector.RequestCouchApp)
    tempWfs = wfDBReader.getRequestByNames(listOfWfs, True)

    for wf in listOfWfs:
        print("%-125s\t%s" % (wf, tempWfs[wf]['RequestStatus']))

    return

def getCondorJobs():
    """
    Retrieve jobs from condor.
    :return: amount of jobs ordered by JobStatus and workflow
    """
    jobDict = {}
    schedd = condor.Schedd()
    jobs = schedd.xquery('true', ['WMAgent_RequestName', 'JobStatus'])
    for job in jobs:
        jobStatus = job['JobStatus']
        jobDict.setdefault(jobStatus, {})
        jobDict[jobStatus].setdefault(job['WMAgent_RequestName'], 0)
        jobDict[jobStatus][job['WMAgent_RequestName']] += 1
    return jobDict


def getWMBSInfo(config):
    """
    blah
    :return:
    """
    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)

    jobsByState = formatter.formatDict(myThread.dbi.processData(jobCountByState))
    print("\n*** Amount of wmbs jobs ordered by status:\n%s" % jobsByState)

    workflows = formatter.formatDict(myThread.dbi.processData(knownWorkflows))
    workflows = [wf['name'] for wf in workflows]
    print("\n*** Distinct workflows known by this agent:")
    getWorkflowStatus(config, workflows)

    wfsNotInjected = formatter.format(myThread.dbi.processData(workflowsNotInjected))
    wfsNotInjected = [wf['name'] for wf in wfsNotInjected]
    print("\n*** Workflows not fully injected:\n")
    getWorkflowStatus(config, wfsNotInjected)


def main():
    """
    Retrieve the following information from the agent:
      1. number of jobs in condor
      2. list of distinct workflows in wmbs_workflow (and their status in reqmgr2)
      3. amount of wmbs jobs in each status
      4. list of workflows not fully injected
      5. list of subscriptions not finished
      6. amount of files available in wmbs
      7. amount of files acquired in wmbs
      8. list of blocks not closed in phedex/dbs
      9. list of files not uploaded to dbs
      10. list of files not injected into phedex, with parent block
      11. list of files not injected into phedex, without parent block
    """
    os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    config = loadConfigurationFile(os.environ["WMAGENT_CONFIG"])

    print("\n*** Jobs in condor sorted by condor job status and workflow:")
    pprint(getCondorJobs())

    getWMBSInfo(config)

    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
