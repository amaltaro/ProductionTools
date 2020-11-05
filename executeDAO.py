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
from WMCore.WMInit import connectToDB


class DummyClass(object):
    def __init__(self):
        # Connecting to DB
        myThread = threading.currentThread()
        connectToDB()
        self.dbi = myThread.dbi

        # Creating DAO stuff for job discovery
        self.daoFactory = DAOFactory(package="WMComponent.RucioInjector.Database",
                                     logger=myThread.logger,
                                     dbinterface=self.dbi)
        self.getUnsubscribedDsets = self.daoFactory(classname="GetUnsubscribedDatasets")
        return

    def loadJobsFromListFull(self):
        unsubscribedDatasets = self.getUnsubscribedDsets.execute()
        print("Results from DAO:\n{}".format(pformat(unsubscribedDatasets)))

        return


if __name__ == '__main__':
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'

    # Job ID summary is:
    # First 2 ids: RECO reading unmerged files (KeepOutput=False in the previous task)
    # 3rd ID is a merge job:
    # 4th ID is a processing job reading merged files
    clsObject = DummyClass()
    results = clsObject.loadJobsFromListFull()
