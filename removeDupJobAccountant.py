"""
__removeDupJobAccountant.py__

It will get the last 1000 lines from JobAccountant/ComponentLog
in order to filter for the *.pkl files from JobCache, which then is
used to retrieve all their output files and saves it in memory.

The next step is to query wmbs_file_details and the x-check
what is there already and in the output files list from the
JobCache pickle files.

In the end it prints the path for the bad pickle file (the one
with duplicate lfn).

Created on Apr 29, 2015.
@author: amaltaro
"""
import sys, os, subprocess
import threading
import logging
from pprint import pprint

from WMCore.FwkJobReport.Report import Report
from WMCore.WMInit import connectToDB
from WMCore.Database.DBFormatter import DBFormatter

getQuery = """
           SELECT lfn FROM wmbs_file_details
           """

def main():
    """
    _main_
    """
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    if 'manage' not in os.environ:
        os.environ['manage'] = '/data/srv/wmagent/current/config/wmagent/manage'

    # first, let's get the last 1000 lines from the component log
    command = ["tail", "-n1000", "install/wmagent/JobAccountant/ComponentLog"]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    logFiles = [line for line in out.split('\n') if 'install/wmagent/JobCreator/JobCache' in line]
    logFiles = [i.split()[2] for i in logFiles]
    print "Found %d pickle files to open" % len(logFiles)
    #pprint(logFiles)

    # second, we unpickle each of these files and get their output files
    dictOutputPkl = {}
    jobReport = Report()
    for pklPath in logFiles:
        if not os.path.exists(pklPath):
            continue
        jobReport.load(pklPath)
        for e in jobReport.getAllFiles():
            dictOutputPkl[e['lfn']] = pklPath
    listOutputPkl = [outFile for outFile in dictOutputPkl]
    print "with a total of %d output files" % len(listOutputPkl)
    #pprint(listOutputPkl)

    # third, we now load lfns from the local database
    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)
    output = myThread.transaction.processData(getQuery)
    lfnsDB = formatter.format(output)
    lfnsDB = [item[0] for item in lfnsDB]
    print "Retrieved %d lfns from wmbs_file_details" % len(lfnsDB)
    #pprint(lfnsDB)

    # fourth, time to do the heavy work
    dup = list(set(listOutputPkl) & set(lfnsDB))
    print "\nFound %d duplicate files: %s" % (len(dup), dup)
    print "The bad pkl files are:"
    for fil in dup:
        print dictOutputPkl[fil]

    print "Remove them, restart the component and be happy!"
    return 0

if __name__ == '__main__':
    sys.exit(main())
