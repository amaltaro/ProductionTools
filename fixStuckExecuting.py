#!/usr/bin/env python
"""
Script used to update an inconsistency problem found in production at July 7, 2016
The problem was that there were much more jobs as 'executing' in wmbs_job than
what actually was in condor (or in the BossAir table, bl_runjob).

The idea is that we mark those jobs 'executing' for more than 6 days as 'jobfailed',
if the workflow is still running. Otherwise we mark jobs as 'cleanout'
"""
import sys
import os
import json
import threading
import logging
import time
import urllib2
import urllib
import httplib
import htcondor as condor
from pprint import pprint
from WMCore.WMInit import connectToDB
from WMCore.Database.DBFormatter import DBFormatter


getJobsExecuting = """
                   SELECT wmbs_workflow.name, count(wmbs_job.id) AS count FROM wmbs_job
                     INNER JOIN wmbs_jobgroup ON wmbs_job.jobgroup = wmbs_jobgroup.id
                     INNER JOIN wmbs_subscription ON wmbs_jobgroup.subscription = wmbs_subscription.id
                     INNER JOIN wmbs_workflow ON wmbs_subscription.workflow = wmbs_workflow.id
                     WHERE wmbs_job.state=(SELECT id from wmbs_job_state where name='executing') 
                     AND wmbs_job.state_time < :timestamp GROUP BY wmbs_workflow.name
                   """

getWMBSIds = """
             SELECT wmbs_job.id FROM wmbs_job
             INNER JOIN wmbs_jobgroup ON wmbs_job.jobgroup = wmbs_jobgroup.id
             INNER JOIN wmbs_subscription ON wmbs_jobgroup.subscription = wmbs_subscription.id
             INNER JOIN wmbs_sub_types ON wmbs_subscription.subtype = wmbs_sub_types.id
             INNER JOIN wmbs_job_state ON wmbs_job.state = wmbs_job_state.id
             INNER JOIN wmbs_workflow ON wmbs_subscription.workflow = wmbs_workflow.id
             WHERE wmbs_workflow.name = :wfname AND wmbs_job.state_time < :timestamp AND wmbs_job_state.name = 'executing'
             """

updateState = """
              UPDATE wmbs_job SET state=(SELECT id from wmbs_job_state WHERE name = :state) WHERE id = :id
              """


CACHE_STATUS = {'fabozzi_HIRun2015-HIMinimumBias5-02May2016_758p4_160502_172625_4322': u'running-closed',
 'pdmvserv_BPH-RunIISummer15GS-00073_00378_v0__160617_003301_5151': u'running-closed',
 'pdmvserv_BPH-RunIISummer15GS-00074_00378_v0__160617_003320_571': u'running-closed',
 'pdmvserv_BPH-Summer12-00203_00262_v0__160617_004010_4907': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00120_00094_v0__160623_125059_340': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00122_00094_v0__160623_125031_7227': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00135_00095_v0__160623_125340_8771': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00152_00097_v0__160624_050737_8732': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00158_00095_v0__160623_125840_5079': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00166_00095_v0__160623_130011_549': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00253_00102_v0__160624_142220_8472': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00566_00083_v0__160622_180022_4786': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00582_00084_v0__160622_180650_1987': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00718_00085_v0__160622_182152_5466': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00742_00085_v0__160622_183733_4944': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00760_00086_v0__160622_184533_1228': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00845_00089_v0__160622_191239_4196': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00856_00089_v0__160622_191544_3744': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00910_00099_v0__160624_125206_2898': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00937_00100_v0__160624_125807_1647': u'running-closed',
 'pdmvserv_EXO-RunIISummer15wmLHEGS-00969_00101_v0__160624_130733_5683': u'running-closed',
 'pdmvserv_JME-RunIISpring16DR80-00006_00369_v0_NZS_160611_055903_7220': u'normal-archived',
 'pdmvserv_SMP-RunIISummer15wmLHEGS-00021_00058_v0__160601_170948_1091': u'running-closed',
 'pdmvserv_TOP-RunIISummer15wmLHEGS-00009_00068_v0__160609_042535_4109': u'aborted-archived',
 'pdmvserv_task_BTV-RunIISpring16DR80-00038__v1_T_160516_093427_2240': u'normal-archived',
 'pdmvserv_task_EXO-RunIISpring16DR80-01891__v1_T_160607_063010_9581': u'running-closed',
 'pdmvserv_task_EXO-RunIISpring16DR80-01897__v1_T_160607_063129_1033': u'running-closed',
 'pdmvserv_task_EXO-RunIISpring16DR80-01991__v1_T_160607_065320_4989': u'running-closed',
 'pdmvserv_task_FSQ-RunIIFall15DR76-00035__v1_T_160529_051059_2431': u'running-closed',
 'pdmvserv_task_FSQ-RunIIFall15DR76-00036__v1_T_160530_091458_9356': u'running-closed',
 'pdmvserv_task_HIG-RunIISpring16DR80-01202__v1_T_160615_123725_1983': u'running-closed',
 'pdmvserv_task_HIG-RunIISpring16DR80-01259__v1_T_160620_125903_751': u'running-closed',
 'pdmvserv_task_HIG-RunIISpring16DR80-01273__v1_T_160621_002508_1213': u'running-closed',
 'pdmvserv_task_TOP-RunIISpring16DR80-00043__v1_T_160611_010715_7552': u'rejected-archived',
 'pdmvserv_task_TOP-RunIISpring16DR80-00048__v1_T_160615_122424_1624': u'normal-archived',
 'pdmvserv_task_TSG-RunIISpring16DR80-00023__v1_T_160614_212137_6781': u'normal-archived',
 'prozober_ACDC_BPH-RunIISpring16DR80-00002_00185_v0__160531_154059_1635': u'aborted-archived',
 'prozober_HIG-RunIISummer15wmLHEGS-00230_00059_v0__160615_164923_3825': u'rejected',
 'vlimant_SUS-RunIISummer15GS-00137_00284_v0__160630_101419_5507': u'running-closed',
 'vlimant_task_SUS-RunIIFall15DR76-00109__v1_T_160609_150914_3262': u'running-closed',
 'vlimant_task_SUS-RunIIFall15DR76-00120__v1_T_160609_143246_6395': u'running-closed',
 'vlimant_task_SUS-RunIIFall15DR76-00121__v1_T_160609_143356_4343': u'running-closed',
 'vlimant_task_SUS-RunIIFall15DR76-00122__v1_T_160609_143434_4853': u'running-closed'}


def getStatus(workflow):
    # for some reason I'm getting a damn SSL error in submit2... 
    # ssl.SSLError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:591)
    global CACHE_STATUS
    if workflow in CACHE_STATUS:
        return CACHE_STATUS.get(workflow)
    url = 'cmsweb.cern.ch'
    headers = {"Accept": "application/json", "Content-type": "application/json"}
    conn = httplib.HTTPSConnection(url, cert_file = os.getenv('X509_USER_PROXY'), key_file = os.getenv('X509_USER_PROXY'))
    urn = "/reqmgr2/data/request/%s" % workflow
    print "Calling urn %s" % urn
    r1=conn.request("GET", urn, headers=headers)
    r2=conn.getresponse()
    request = json.loads(r2.read())["result"][0]
    return request[workflow]['RequestStatus']


def main():
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    if 'manage' not in os.environ:
        os.environ['manage'] = '/data/srv/wmagent/current/config/wmagent/manage'

    getStatus("fabozzi_HIRun2015-HIMinimumBias5-02May2016_758p4_160502_172625_4322")
    timenow = int(time.time())
    time6d = timenow - 6 * 24 * 3600

    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)

    # Get list of workflows and number of jobs executing for more than 6 days
    binds = [{'timestamp': time6d}]
    wmbsJobsPerWf = formatter.formatDict(myThread.dbi.processData(getJobsExecuting, binds))
    totalJobs = sum([int(item['count']) for item in wmbsJobsPerWf])
    print "Found %d workflows with a total of %d jobs" % (len(wmbsJobsPerWf), totalJobs)

    # Retrieve all jobs from condor schedd
    schedd = condor.Schedd()
    jobs = schedd.xquery('true', ['ClusterID', 'ProcId', 'WMAgent_RequestName', 'JobStatus', 'WMAgent_JobID'])

    # Retrieve their status from reqmgr2 and
    # add their wmbsId to the dict
    for item in wmbsJobsPerWf:
        item['status'] = getStatus(item['name'])
        item['condorjobs'] = []
        for job in jobs:
            if job['WMAgent_RequestName'] == item['name']:
                item['condorjobs'].append(job['WMAgent_JobID'])

    #pprint(wmbsJobsPerWf)

    # time to have some ACTION
    for item in wmbsJobsPerWf:
        binds = [{'timestamp': time6d, 'wfname': item['name']}]
        jobIds  = formatter.formatDict(myThread.dbi.processData(getWMBSIds, binds))
        wmbsIds = [x['id'] for x in jobIds]
        print "%-100s in %s. Has %d wmbs and %d condor jobs" % (item['name'], item['status'], len(wmbsIds), len(item['condorjobs']))
        # Just skip it if there are condor jobs out there
        if len(item['condorjobs']) > 0:
            continue
        newstatus = 'jobfailed' if item['status'] in ('acquired', 'running-open', 'running-closed') else 'cleanout'
        var = raw_input("Marking jobs from %s to %s: (Y/N) " % (item['status'], newstatus))
        if var in ['Y', 'y']:
            print "UPDATED %s" % item['name']
            binds = []
            for x in jobIds:
                x['state'] = newstatus
                binds.append(x)
            myThread.dbi.processData(updateState, binds)

    print "Done!"
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
