"""
Requirements to run this script:
 * condor_schedd daemon

Jobs are actually submitted to the local condor queue, but they won't
run because the x509 ads were not properly set (see REPLACE-ME)
"""
from __future__ import print_function

import os
import re
import sys
import time
import uuid
import collections
from copy import deepcopy

import psutil
import classad
import htcondor
#from memory_profiler import profile

TEST_DIR = '/data/srv/wmagent/current/alanTest/'
reqStr = ('((REQUIRED_OS=?="any") || '
          '(GLIDEIN_REQUIRED_OS =?= "any") || '
          'stringListMember(GLIDEIN_REQUIRED_OS, REQUIRED_OS)) && '
          '(AuthenticatedIdentity =!= "volunteer-node@cern.ch")')

x509Expr = 'ifThenElse("$$(GLIDEIN_CMSSite)" =?= "T3_CH_Volunteer",undefined,"%s")'


def convertFromUnicodeToStr(data):
    """
    code fram
    http://stackoverflow.com/questions/1254454/fastest-way-to-convert-a-dicts-keys-values-from-unicode-to-str
    """
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(list(map(convertFromUnicodeToStr, data.iteritems())))
    elif isinstance(data, collections.Iterable):
        return type(data)(list(map(convertFromUnicodeToStr, data)))
    else:
        return data


def makeUUID():
    """
    _makeUUID_

    Makes a UUID from the uuid class, returns it
    """
    return str(uuid.uuid4())


def getClusterAd():
    """ Mimic the same method of SimpleCondorPlugin """
    ad = classad.ClassAd()

    # ad['universe'] = "vanilla"
    ad['ShouldTransferFiles'] = "YES"
    ad['WhenToTransferOutput'] = "ON_EXIT"
    ad['UserLogUseXML'] = True
    ad['JobNotification'] = 0
    ad['Cmd'] = TEST_DIR + 'submit_fake.sh'
    # Investigate whether we should pass the absolute path for Out and Err ads,
    # just as we did for UserLog. There may be issues, more info on WMCore #7362
    ad['Out'] = classad.ExprTree('strcat("condor.", ClusterId, ".", ProcId, ".out")')
    ad['Err'] = classad.ExprTree('strcat("condor.", ClusterId, ".", ProcId, ".err")')
    ad['UserLog'] = classad.ExprTree('strcat(Iwd, "/condor.", ClusterId, ".", ProcId, ".log")')

    ad['WMAgent_AgentName'] = 'WMAgentCommissioning'

    ad['JobLeaseDuration'] = classad.ExprTree(
        'isUndefined(MachineAttrMaxHibernateTime0) ? 1200 : MachineAttrMaxHibernateTime0')

    ad['PeriodicRemove'] = classad.ExprTree('( JobStatus =?= 5 ) && ( time() - EnteredCurrentStatus > 10 * 60 )')
    removeReasonExpr = 'PeriodicRemove ? "Job automatically removed for being in Held status" : ""'
    ad['PeriodicRemoveReason'] = classad.ExprTree(removeReasonExpr)

    # Required for global pool accounting
    ad['AcctGroup'] = 'production'
    ad['AcctGroupUser'] = 'cmsdataops'
    ad['AccountingGroup'] = "%s.%s" % (ad['AcctGroup'], ad['AcctGroupUser'])

    # Customized classAds for this plugin
    ad['DESIRED_Archs'] = "INTEL,X86_64"

    ad['Rank'] = 0.0
    ad['TransferIn'] = False

    ad['JobMachineAttrs'] = "GLIDEIN_CMSSite"
    ad['JobAdInformationAttrs'] = ("JobStatus,QDate,EnteredCurrentStatus,JobStartDate,DESIRED_Sites,"
                                   "ExtDESIRED_Sites,WMAgent_JobID,MachineAttrGLIDEIN_CMSSite0")

    # TODO: remove when 8.5.7 is deployed
    paramsToAdd = htcondor.param['SUBMIT_ATTRS'].split() + htcondor.param['SUBMIT_EXPRS'].split()
    paramsToSkip = ['accounting_group', 'use_x509userproxy', 'PostJobPrio2', 'JobAdInformationAttrs']
    for param in paramsToAdd:
        if (param not in ad) and (param in htcondor.param) and (param not in paramsToSkip):
            ad[param] = classad.ExprTree(htcondor.param[param])
    return ad


def getProcAds(jobList):
    """ Mimic getProcAds method from SimpleCondorPlugin """
    classAds = []
    for job in jobList:
        ad = {}

        ad['Iwd'] = job['cache_dir']
        ad['TransferInput'] = "%s,%s/%s,%s" % (job['sandbox'], job['packageDir'],
                                               'JobPackage.pkl', TEST_DIR + 'src/python/WMCore/WMRuntime/Unpacker.py')
        ad['Arguments'] = "%s %i" % (os.path.basename(job['sandbox']), job['id'])

        ad['TransferOutput'] = "Report.%i.pkl,wmagentJob.log" % job["retry_count"]

        # Do not define Requirements and X509 ads for Volunteer resources
        if reqStr and "T3_CH_Volunteer" not in job.get('possibleSites'):
            ad['Requirements'] = classad.ExprTree(reqStr)

        ad['x509userproxy'] = classad.ExprTree(x509Expr % "/tmp/proxy.pem")
        ad['x509userproxysubject'] = classad.ExprTree(
            x509Expr % "REPLACE-ME")
        ad['x509userproxyfirstfqan'] = classad.ExprTree(x509Expr % "REPLACE-ME")

        sites = ','.join(sorted(job.get('possibleSites')))
        ad['DESIRED_Sites'] = sites

        sites = ','.join(sorted(job.get('potentialSites')))
        ad['ExtDESIRED_Sites'] = sites

        ad['CMS_JobRetryCount'] = job['retry_count']
        ad['WMAgent_RequestName'] = job['request_name']

        match = re.compile("^[a-zA-Z0-9_]+_([a-zA-Z0-9]+)-").match(job['request_name'])
        if match:
            ad['CMSGroups'] = match.groups()[0]
        else:
            ad['CMSGroups'] = classad.Value.Undefined

        ad['WMAgent_JobID'] = job['jobid']
        ad['WMAgent_SubTaskName'] = job['task_name']
        ad['CMS_JobType'] = job['task_type']

        # Handling for AWS, cloud and opportunistic resources
        ad['AllowOpportunistic'] = job.get('allowOpportunistic', False)

        if job.get('inputDataset'):
            ad['DESIRED_CMSDataset'] = job['inputDataset']
        else:
            ad['DESIRED_CMSDataset'] = classad.Value.Undefined

        if job.get('inputDatasetLocations'):
            sites = ','.join(sorted(job['inputDatasetLocations']))
            ad['DESIRED_CMSDataLocations'] = sites
        else:
            ad['DESIRED_CMSDataLocations'] = classad.Value.Undefined

        # HighIO and repack jobs
        ad['Requestioslots'] = 1 if job['task_type'] in ["Merge", "Cleanup", "LogCollect"] else 0
        ad['RequestRepackslots'] = 1 if job['task_type'] == 'Repack' else 0

        # Performance and resource estimates (including JDL magic tweaks)
        origCores = job.get('numberOfCores', 1)
        estimatedMins = int(job['estimatedJobTime'] / 60.0) if job.get('estimatedJobTime') else 12 * 60
        estimatedMinsSingleCore = estimatedMins * origCores
        # For now, assume a 15 minute job startup overhead -- condor will round this up further
        ad['EstimatedSingleCoreMins'] = estimatedMinsSingleCore
        ad['OriginalMaxWallTimeMins'] = estimatedMins
        ad['MaxWallTimeMins'] = classad.ExprTree(
            'WMCore_ResizeJob ? (EstimatedSingleCoreMins/RequestCpus + 15) : OriginalMaxWallTimeMins')

        requestMemory = int(job['estimatedMemoryUsage']) if job.get('estimatedMemoryUsage', None) else 1000
        ad['OriginalMemory'] = requestMemory
        ad['ExtraMemory'] = 500
        ad['RequestMemory'] = classad.ExprTree(
            'OriginalMemory + ExtraMemory * (WMCore_ResizeJob ? (RequestCpus-OriginalCpus) : 0)')

        requestDisk = int(job['estimatedDiskUsage']) if job.get('estimatedDiskUsage',
                                                                None) else 20 * 1000 * 1000 * origCores
        ad['RequestDisk'] = requestDisk

        # Set up JDL for multithreaded jobs.
        # By default, RequestCpus will evaluate to whatever CPU request was in the workflow.
        # If the job is labelled as resizable, then the logic is more complex:
        # - If the job is running in a slot with N cores, this should evaluate to N
        # - If the job is being matched against a machine, match all available CPUs, provided
        # they are between min and max CPUs.
        # - Otherwise, just use the original CPU count.
        ad['MinCores'] = int(job.get('minCores', max(1, origCores / 2)))
        ad['MaxCores'] = max(int(job.get('maxCores', origCores)), origCores)
        ad['OriginalCpus'] = origCores
        # Prefer slots that are closest to our MaxCores without going over.
        # If the slot size is _greater_ than our MaxCores, we prefer not to
        # use it - we might unnecessarily fragment the slot.
        ad['Rank'] = classad.ExprTree('isUndefined(Cpus) ? 0 : ifThenElse(Cpus > MaxCores, -Cpus, Cpus)')
        # Record the number of CPUs utilized at match time.  We'll use this later
        # for monitoring and accounting.  Defaults to 0; once matched, it'll
        # put an attribute in the job  MATCH_EXP_JOB_GLIDEIN_Cpus = 4
        ad['JOB_GLIDEIN_Cpus'] = "$$(Cpus:0)"
        # Make sure the resize request stays within MinCores and MaxCores.
        ad['RequestResizedCpus'] = classad.ExprTree(
            '(Cpus>MaxCores) ? MaxCores : ((Cpus < MinCores) ? MinCores : Cpus)')
        # If the job is running, then we should report the matched CPUs in RequestCpus - but only if there are sane
        # values.  Otherwise, we just report the original CPU request
        ad['JobCpus'] = classad.ExprTree(
            '((JobStatus =!= 1) && (JobStatus =!= 5) && !isUndefined(MATCH_EXP_JOB_GLIDEIN_Cpus) '
            '&& (int(MATCH_EXP_JOB_GLIDEIN_Cpus) isnt error)) ? int(MATCH_EXP_JOB_GLIDEIN_Cpus) : OriginalCpus')

        # Cpus is taken from the machine ad - hence it is only defined when we are doing negotiation.
        # Otherwise, we use either the cores in the running job (if available) or the original cores.
        ad['RequestCpus'] = classad.ExprTree(
            'WMCore_ResizeJob ? (!isUndefined(Cpus) ? RequestResizedCpus : JobCpus) : OriginalCpus')
        ad['WMCore_ResizeJob'] = bool(job.get('resizeJob', False))

        taskPriority = int(job.get('taskPriority', 0))
        priority = int(job.get('wf_priority', 0))
        ad['JobPrio'] = int(priority + taskPriority * 1e7)
        ad['PostJobPrio1'] = int(-1 * len(job.get('potentialSites', [])))
        ad['PostJobPrio2'] = int(-1 * job['task_id'])

        # Add OS requirements for jobs
        # requiredOSes = self.scramArchtoRequiredOS(job.get('scramArch'))
        ad['REQUIRED_OS'] = "rhel6"
        cmsswVersions = ','.join(job.get('swVersion'))
        ad['CMSSW_Versions'] = cmsswVersions

        ad = convertFromUnicodeToStr(ad)
        condorAd = classad.ClassAd()
        for k, v in ad.iteritems():
            condorAd[k] = v
        classAds.append((condorAd, 1))

    return classAds


def getJobs(numJobs=1):
    """
    Return a list of dictionary data as provided to the plugin `submit` method
    """
    job = {'allowOpportunistic': False,
           'bulkid': None,
           'cache_dir': TEST_DIR + '/JobCollection_1_0/job_1',
           'estimatedDiskUsage': 5000000,
           'estimatedJobTime': 28800,
           'estimatedMemoryUsage': 6000.0,
           'gridid': None,
           'id': 1L,
           'inputDataset': '/HLTPhysics/Run2017B-PromptReco-v1/AOD',
           'inputDatasetLocations': ['T2_CH_CERN_HLT', 'T2_CH_CERN'],
           'jobid': 1L,
           'location': 'T2_CH_CERN',
           'name': '934a7f0d-2934-4939-b366-0a9efe0df15e-0',
           'numberOfCores': 8,
           'packageDir': TEST_DIR + '/batch_1-0',
           'plugin': 'SimpleCondorPlugin',
           'possibleSites': [u'T2_CH_CERN', u'T1_US_FNAL'],
           'potentialSites': frozenset([u'T1_US_FNAL', u'T2_CH_CERN']),
           'proxyPath': None,
           'request_name': 'amaltaro_test_submission_180620_105409_2045',
           'retry_count': 0L,
           'sandbox': TEST_DIR + '/Blah-Sandbox.tar.bz2',
           'scramArch': ['slc6_amd64_gcc630'],
           'siteName': u'T2_CH_CERN',
           'site_cms_name': 'T2_CH_CERN',
           'status': None,
           'status_time': None,
           'swVersion': ['CMSSW_9_4_0'],
           'taskPriority': 0L,
           'task_id': 383L,
           'task_name': '/amaltaro_test_submission_180620_105409_2045/Blah_Task',
           'task_type': 'Processing',
           'userdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=amaltaro/CN=718748/CN=Alan Malta Rodrigues',
           'usergroup': 'unknown',
           'userrole': 'unknown',
           'wf_priority': 420000L}

    jobs = []
    for i in range(0, numJobs):
        job.update({'id': long(i), 'jobid': long(i), 'name': makeUUID()})
        jobs.append(deepcopy(job))
    return jobs


#profileFp = open('b.log', 'w+')
#@profile(stream=profileFp)
def submitJobs():
    """Create all the necessary objects and submit to condor"""
    successfulJobs = []
    jobsReady = getJobs(1000)
    schedd = htcondor.Schedd()
    clusterAd = getClusterAd()
    procAds = getProcAds(jobsReady)

    print("Start: Submitting %d jobs using Condor Python SubmitMany" % len(procAds))
    clusterId = schedd.submitMany(clusterAd, procAds)
    print("Finish: Submitting jobs using Condor Python SubmitMany\n")

    for index, job in enumerate(jobsReady):
        job['gridid'] = "%s.%s" % (clusterId, index)
        job['status'] = 'Idle'
        successfulJobs.append(job)
        # print("Successful jobs were: %s" % successfulJobs)


def main():
    # make it a different function such that gc can collect objects
    thisProcess = psutil.Process(os.getpid())
    print("Initial memory RSS: %s" % thisProcess.memory_info().rss)
    for i in range(1, 21):
        print("Running %i submission cycle" % i)
        submitJobs()
        print("Current memory RSS: %s" % thisProcess.memory_info().rss)
        time.sleep(10)  # give enough time to gc to collect objects, if needed...
    print("Final memory RSS: %s" % thisProcess.memory_info().rss)


if __name__ == "__main__":
    sys.exit(main())
