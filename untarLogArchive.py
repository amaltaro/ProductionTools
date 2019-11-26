#!/usr/bin/env python
"""
This script is meant to be used inside an agent in order to find
the correct log file that matches something. For example, to find
which job processed a given file (changes have to be made to the script).

Use it with care, it creates a reasonable IO load on the node because
it's actually going to:
* list all Cache directories
* for each Cache directories, it lists all logArchive tarballs
* untar every tarball - in memory - and match your string against
the wmagentJob.log. If there is a match, print the log name.

E.g. of usage:
python untarLogArchive.py /data/srv/wmagent/current/install/wmagent/JobArchiver/logDir/p/pdmvserv_task_HIG-RunIISummer19UL17wmLHEGEN-00493__v1_T_191030_134608_301/
"""
from __future__ import print_function

import os
import sys
import tarfile
import time


def extractFile(logArchName):
    """
    Extract the file and returns the xml data
    """
    jobName = logArchName.rsplit('/')[-1]
    fName = '%s/wmagentJob.log' % jobName.replace(".tar.bz2", "")
    try:
        with tarfile.open(logArchName, 'r:bz2') as tf:
            jobLog = tf.extractfile(fName)
            rawdata = jobLog.read()
    except IOError:
        print("Failed to untar %s" % logArchName)
    except Exception:
        print("Unknown failure untarring %s" % logArchName)
    else:
        # if "T2_PK_NCP" in rawdata:
        if "6420a2d1-6f71-4aea-bbb7-955e67a1db05-0-2-logArchive.tar.gz" in rawdata:
            print(logArchName)
    return


dirPath = sys.argv[1]
subDirs = os.listdir(dirPath)
print("Found %d sub-directories under: %s" % (len(subDirs), dirPath))
for subDir in subDirs:
    newDir = os.path.join(dirPath, subDir)
    files = os.listdir(newDir)
    print("Found %d files under : %s" % (len(files), newDir))
    for fName in files:
        extractFile(os.path.join(newDir, fName))
    time.sleep(5)
    print("\n\n")
sys.exit(0)
