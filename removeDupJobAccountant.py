"""
__removeDupJobAccountant.py__

It will get the last 1000 lines from JobAccountant/ComponentLog and parse it
looking for Report.*.pkl files.

Then it loads every one of those pickle files and create a list of all their
output files.

Next, it loads all LFNs from the wmbs_file_details table.

Finally, it does an intersection of the pickle output files against files
in the wmbs table. Printing out files that already exist in the database
and the pickle file names containing the same lfns.

Created on Apr 29, 2015.
@author: amaltaro
"""
from __future__ import print_function

import sys
import os
import subprocess
import threading
import logging
import json
from pprint import pformat

from WMCore.FwkJobReport.Report import Report
from WMCore.WMInit import connectToDB
from WMCore.Database.DBFormatter import DBFormatter


def main():
    """
    _main_

    """
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    if 'manage' not in os.environ:
        os.environ['manage'] = '/data/srv/wmagent/current/config/wmagent/manage'

    ### Fetch the report pickle files from the component log
    command = ["tail", "-n1000", "install/wmagent/JobAccountant/ComponentLog"]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    logFiles = [line for line in out.splitlines() if 'install/wmagent/JobCreator/JobCache' in line]
    logFiles = [i.split()[2] for i in logFiles]
    msg = "Found %d pickle files to parse " % len(logFiles)

    ### Now unpickle each of these files and get their output files
    # also check whether any of them are duplicate
    lfn2PklDict = {}
    dupOutputPkl = {}  # string value with the dup LFN and keyed by the pickle file path
    jobReport = Report()
    for pklPath in logFiles:
        if not os.path.exists(pklPath):
            continue

        jobReport.load(pklPath)
        for e in jobReport.getAllFiles():
            lfn2PklDict.setdefault(e['lfn'], [])
            lfn2PklDict[e['lfn']].append(pklPath)

    # now check which files contain more than one pickle path (= created by diff jobs)
    dupFiles = []
    for lfn, pkls in lfn2PklDict.iteritems():
        if len(pkls) > 1:
            dupFiles.append(lfn)
            for pkl in pkls:
                if pkl not in dupOutputPkl:
                    jobReport.load(pkl)
                    dupOutputPkl[pkl] = jobReport.__to_json__(None)
                    dupOutputPkl[pkl]['dup_lfns'] = []
                dupOutputPkl[pkl]['dup_lfns'].append(lfn)

    msg += "with a total of %d output files and %d duplicated" % (len(lfn2PklDict), len(dupFiles))
    msg += " files to process among them."
    msg += "\nDuplicate files are:\n%s" % dupFiles
    print(msg)

    if dupFiles:
        print("See dupPickles.json for further details ...")
        with open('dupPickles.json', 'w') as fo:
            json.dump(dupOutputPkl, fo, indent=2)

    if dupFiles:
        var = raw_input("Can we automatically delete those pickle files? Y/N\n")
        if var == "Y":
            # then delete all job report files but the first one - NOT ideal
            for fname in dupFiles:
                for pklFile in lfn2PklDict[fname][1:]:
                    if os.path.isfile(pklFile):
                        print("Deleting %s ..." % pklFile)
                        os.remove(pklFile)
                    else:
                        print("    File has probably been already deleted %s ..." % pklFile)
            print("  Done!")

    ### Time to load all - this is BAD - LFNs from WMBS database
    print("\nNow loading all LFNs from wmbs_file_details ...")
    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)
    output = myThread.transaction.processData("SELECT lfn FROM wmbs_file_details")
    lfnsDB = formatter.format(output)
    lfnsDB = [item[0] for item in lfnsDB]
    print("Retrieved %d lfns from wmbs_file_details" % len(lfnsDB))

    ### Compare what are the duplicates
    dupFiles = list(set(lfn2PklDict.keys()) & set(lfnsDB))
    print("\nFound %d duplicate files." % len(dupFiles))
    if len(dupFiles) == 0:
        sys.exit(0)

    ### Print some basic data about these reports
    print("Their overview is: ")
    dbDupPkl = []
    for fname in dupFiles:
        for pklPath in lfn2PklDict[fname]:
            jobInfo = {'lfn': fname}
            jobInfo['pklPath'] = pklPath

            try:
                jobReport.load(pklPath)
            except IOError:
                # pkl file has been deleted already
                continue
            jobInfo['exitCode'] = jobReport.getExitCode()
            jobInfo['taskSuccess'] = jobReport.taskSuccessful()
            jobInfo['EOSLogURL'] = jobReport.getLogURL()
            jobInfo['HostName'] = jobReport.getWorkerNodeInfo()['HostName']
            jobInfo['Site'] = jobReport.getSiteName()
            jobInfo['task'] = jobReport.getTaskName()

            dbDupPkl.append(jobInfo)

    print(pformat(dbDupPkl))
    print("")

    print("Remove them, restart the component and be happy!\n")
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
