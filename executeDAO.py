#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script using the same logic as in ErrorHandler to load job
information from the database, using the DAOFactory
"""
from __future__ import print_function, division

import os
import threading
from pprint import pformat

from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Job import Job
from WMCore.WMInit import connectToDB


class DummyClass(object):
    def __init__(self):
        # Connecting to DB
        myThread = threading.currentThread()
        connectToDB()
        self.dbi = myThread.dbi

        # Creating DAO stuff for job discovery
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=self.dbi)
        self.loadAction = self.daoFactory(classname="Jobs.LoadForErrorHandler")
        return

    def loadJobsFromListFull(self, idList):
        binds = []
        for jobID in idList:
            binds.append({"jobid": jobID})

        results = self.loadAction.execute(jobID=binds)

        # You have to have a list
        if isinstance(results, dict):
            results = [results]
        print("Results from DAO:\n{}".format(pformat(results)))

        listOfJobs = []
        for entry in results:
            # One job per entry
            tmpJob = Job(id=entry['id'])
            tmpJob.update(entry)
            listOfJobs.append(tmpJob)

        return listOfJobs


if __name__ == '__main__':
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'

    # Job ID summary is:
    # First 2 ids: RECO reading unmerged files (KeepOutput=False in the previous task)
    # 3rd ID is a merge job:
    # 4th ID is a processing job reading merged files
    listOfJobIds = [3586334, 3586335, 3532541, 3418998]  # [3419478, 3418998]
    clsObject = DummyClass()
    results = clsObject.loadJobsFromListFull(listOfJobIds)
    print("\n\nResults from loadJobsFromListFull: {}".format(pformat(results)))
    # adding the job Mask
    print("\n\nResults after mask is applied")
    for job in results:
        job.getMask()
        print(pformat(job))
