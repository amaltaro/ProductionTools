#!/usr/bin/env python
"""
Gets a list of workflows and lfns (taken from an agent that is about to be
redeployed) and:
 1. make a list of datasets out of the lfns passed in
 2. check if those datasets belong to any of the workflows passed in (known
 by the agent)
 3. print whether it's an input or output dataset of a know workflow
 """
from __future__ import print_function

import sys
import os
import threading
import logging
from pprint import pprint, pformat

try:
    import htcondor as condor
    from Utils.IterTools import flattenList
    from WMCore.WMInit import connectToDB
    from WMCore.Database.DBFormatter import DBFormatter
    from WMCore.Configuration import loadConfigurationFile
    from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
    # from WMCore.DAOFactory import DAOFactory
except ImportError:
    print("You do not have a proper environment, please source the following:")
    print("source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh")
    sys.exit(1)

jobCountByState = """
    select wmbs_job_state.name, count(*) AS count
      from wmbs_job
      join wmbs_job_state on (wmbs_job.state = wmbs_job_state.id)
      group by wmbs_job.state, wmbs_job_state.name
    """

knownWorkflows = "SELECT DISTINCT name from wmbs_workflow"

workflowsNotInjected = "select distinct name from wmbs_workflow where injected = 0"

unfinishedSubscriptions = """
   select distinct wmbs_workflow.name AS wfName
     FROM wmbs_subscription
     INNER JOIN wmbs_fileset ON wmbs_subscription.fileset = wmbs_fileset.id
     INNER JOIN wmbs_workflow ON wmbs_workflow.id = wmbs_subscription.workflow
     where wmbs_subscription.finished = 0 ORDER BY wmbs_workflow.name
   """

filesAvailWMBS = "select subscription,count(*) from wmbs_sub_files_available group by subscription"

filesAcqWMBS = "select subscription,count(*) from wmbs_sub_files_acquired group by subscription"

blocksOpenDBS = "SELECT * FROM dbsbuffer_block WHERE status!='Closed'"

filesNotInDBS = "SELECT * from dbsbuffer_file where status = 'NOTUPLOADED'"

filesNotInPhedex = """
    SELECT lfn FROM dbsbuffer_file
      WHERE in_phedex=0
      AND block_id IS NOT NULL
      AND lfn NOT LIKE '%%unmerged%%'
      AND lfn NOT LIKE 'MCFakeFile%%'
      AND lfn NOT LIKE '%%BACKFILL%%'
      AND lfn NOT LIKE '/store/backfill/%%'
      AND lfn NOT LIKE '/store/user%%'
    """
filesNotInPhedexNull = """
    SELECT lfn FROM dbsbuffer_file
      WHERE in_phedex=0
      AND block_id IS NULL
      AND lfn NOT LIKE '%%unmerged%%'
      AND lfn NOT LIKE 'MCFakeFile%%'
      AND lfn NOT LIKE '%%BACKFILL%%'
      AND lfn NOT LIKE '/store/backfill/%%'
      AND lfn NOT LIKE '/store/user%%'
    """


def printWfStatus(wfs, workflowsDict):
    """
    Given a list of request names, print their RequestStatus from the short dictionary.
    """
    for wf in wfs:
        print("%-125s\t%s" % (wf, workflowsDict[wf]['RequestStatus']))

    return


def lfn2dset(lfns):
    """ Convert a LFN into a dataset name """
    if isinstance(lfns, basestring):
        lfns = [lfns]

    listDsets = set()
    for lfn in lfns:
        toks = lfn.split('/')[3:]
        dset = "/%s/%s-%s/%s" % (toks[1], toks[0], toks[3], toks[2])
        listDsets.add(dset)

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


def getDsetAndWf(lfns, wfsDict):
    """
    Given a list of lfns (files not injected in DBS/TMDB), get their dataset
    name and compare against the outputdatasets of workflows known by this agent.

    If there is no match, then the workflow has been archived already and there
    is nothing to worry about.

    If the workflow is known, then we may need to recover these files.
    """
    if not lfns:
        return
    uniqLfns = set([lfn.rsplit('/', 2)[0] for lfn in lfns])
    uniqDsets = lfn2dset(uniqLfns)
    print("==> Which map to %d unique datasets:\n%s" % (len(uniqLfns), pformat(uniqDsets)))

    match = []
    for dset in uniqDsets:
        for wf, values in wfsDict.iteritems():
            if dset in values['OutputDatasets']:
                match.append((wf, values['RequestStatus'], dset))
    if match:
        print("... that were produced by the following workflow, status and the dataset itself:\n")
        pprint(match)
    else:
        print("... that were NOT produced by any agent-known workflow or wfs that are gone from the system.\n")

    return


def fetchWorkflowsSpec(config, listOfWfs):
    """
    Fetch the workload of a list of workflows. Filter out only a few
    usefull keys
    """
    if isinstance(listOfWfs, basestring):
        listOfWfs = [listOfWfs]

    wfDBReader = RequestDBReader(config.AnalyticsDataCollector.centralRequestDBURL,
                                 couchapp=config.AnalyticsDataCollector.RequestCouchApp)
    tempWfs = wfDBReader.getRequestByNames(listOfWfs, True)

    wfShortDict = {}
    for wf in listOfWfs:
        wfShortDict[wf] = filterKeys(tempWfs[wf])

    return wfShortDict


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
    print("\n*** WMBS: amount of wmbs jobs in each status:\n%s" % jobsByState)

    workflows = formatter.formatDict(myThread.dbi.processData(knownWorkflows))
    workflows = [wf['name'] for wf in workflows]
    print("\n*** WORKFLOWS: found %d distinct workflows in this agent." % len(workflows))
    workflowsDict = fetchWorkflowsSpec(config, workflows)
    printWfStatus(workflows, workflowsDict)

    wfsNotInjected = formatter.format(myThread.dbi.processData(workflowsNotInjected))
    wfsNotInjected = [wf['name'] for wf in wfsNotInjected]
    print("\n*** WORKFLOWS: found %d workflows not fully injected.\n" % len(wfsNotInjected))
    printWfStatus(wfsNotInjected, workflowsDict)

    unfinishedSubs = formatter.formatDict(myThread.dbi.processData(unfinishedSubscriptions))
    print("\n*** SUBSCRIPTIONS: subscriptions not finished:\n%s" % unfinishedSubs)

    filesAvailable = formatter.formatDict(myThread.dbi.processData(filesAvailWMBS))
    print("\n*** SUBSCRIPTIONS: found %d files available in WMBS (waiting for job creation):\n%s" % (len(filesAvailable),
                                                                                                     filesAvailable))

    filesAcquired = formatter.formatDict(myThread.dbi.processData(filesAcqWMBS))
    print("\n*** SUBSCRIPTIONS: found %d files acquired in WMBS (waiting for jobs to finish):\n%s" % (len(filesAcquired),
                                                                                                      filesAcquired))

    blocksopenDBS = formatter.formatDict(myThread.dbi.processData(blocksOpenDBS))
    print("\n*** DBS: found %d blocks open in DBS." % len(blocksopenDBS), end="")
    print(" Printing the first 20 blocks only:\n%s" % blocksopenDBS[:20])

    filesnotinDBS = formatter.formatDict(myThread.dbi.processData(filesNotInDBS))
    print("\n*** DBS: found %d files not uploaded to DBS." % len(filesnotinDBS), end="")
    print(" Printing the first 20 lfns only:\n%s" % filesnotinDBS[:20])

    filesnotinPhedex = flattenList(formatter.format(myThread.dbi.processData(filesNotInPhedex)))
    print("\n*** PHEDEX: found %d files not injected in PhEDEx, with valid block id (recoverable)." % len(filesnotinPhedex))
    getDsetAndWf(filesnotinPhedex, workflowsDict)

    filesnotinPhedexNull = flattenList(formatter.format(myThread.dbi.processData(filesNotInPhedexNull)))
    print("\n*** PHEDEX: found %d files not injected in PhEDEx, with valid block id (unrecoverable)." % len(filesnotinPhedexNull))
    getDsetAndWf(filesnotinPhedexNull, workflowsDict)


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

    print("\n*** Amount of jobs in condor per workflow, sorted by condor job status:")
    pprint(getCondorJobs())

    getWMBSInfo(config)

    print("\nI'm done!")
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())

###################### Alan notes ########################
"""
1. Failing subscription files available for workflows that we no longer care:
INSERT INTO wmbs_sub_files_failed (fileid, subscription)
  SELECT fileid, subscription from wmbs_sub_files_available WHERE subscription = 20337;

DELETE FROM wmbs_sub_files_available WHERE subscription = 20337;
"""
