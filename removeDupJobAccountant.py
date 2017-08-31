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

    Pass either -v or --verbose to get verbose output :)
    """
    ### Initial setup
    verbose = True if len(sys.argv) == 2 else False
    verboseData = []
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
    print("Found %d pickle files to parse" % len(logFiles))

    ### Now unpickle each of these files and get their output files
    dictOutputPkl = {}
    jobReport = Report()
    for pklPath in logFiles:
        if not os.path.exists(pklPath):
            continue
        jobReport.load(pklPath)
        for e in jobReport.getAllFiles():
            dictOutputPkl[e['lfn']] = pklPath
    listOutputPkl = [outFile for outFile in dictOutputPkl]
    print("with a total of %d output files" % len(listOutputPkl))

    ### Time to load all - this is BAD - LFNs from WMBS database
    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)
    output = myThread.transaction.processData("SELECT lfn FROM wmbs_file_details")
    lfnsDB = formatter.format(output)
    lfnsDB = [item[0] for item in lfnsDB]
    print("Retrieved %d lfns from wmbs_file_details" % len(lfnsDB))

    ### Compare what are the duplicates
    dupFiles = list(set(listOutputPkl) & set(lfnsDB))
    print("\nFound %d duplicate files:\n%s" % (len(dupFiles), pformat(dupFiles)))
    badFiles = sorted([dictOutputPkl[fil] for fil in dupFiles])
    print("Corresponding to %d bad pkl files, they are:" % len(badFiles))
    for filename in badFiles:
        print(filename)
    print("")

    ### Bonus, go deeper and check the status of these pickle files
    jobReport = Report()
    for f in badFiles:
        print("Parsing %s ..." % f)
        jobReport.load(f)
        jobSucceeded = jobReport.taskSuccessful()
        print("  Successful job: %s" % jobSucceeded)
        if not jobSucceeded:
            print("  Job exit codes are: %s" % jobReport.getExitCodes())
            if verbose:
                verboseData.append(jobReport.__to_json__(None))

    if verbose:
        print("\nDumping all the FJRs content into verboseReport.json\n")
        with open('verboseReport.json', 'w') as jo:
            json.dump(verboseData, jo, indent=2)

    print("Remove them, restart the component and be happy!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
