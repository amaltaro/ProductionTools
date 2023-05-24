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


def extractFile(logArchName, fileId):
    """
    Extract wmagentJob.log file from the tarball and scan for a string pattern

    :param logArchName: string with the absolute path to a log tarball
    :param fileId: string with a file id (or a fraction of it)
    :return: print the log tarball that matches the string pattern
    """
    jobName = logArchName.rsplit('/')[-1]
    fName = '%s/wmagentJob.log' % jobName.replace(".tar.bz2", "")
    try:
        # note that data is loaded as a bytes data type
        with tarfile.open(logArchName, 'r:bz2') as tf:
            jobLog = tf.extractfile(fName)
            rawdata = jobLog.read()
    except IOError:
        print(f"Failed to untar {logArchName}")
    except Exception:
        print(f"Unknown failure untarring {logArchName}")
    else:
        if fileId in rawdata:
            print(f"  MATCH! LogArchive {logArchName}")
            sys.exit(0)
    return

if len(sys.argv) != 3:
    print("Error: you must provide a directory path and file id.")
    print("    e.g.: untarLogArchive /data/srv/wmagent/current/install/wmagent/JobArchiver/logDir/blah fileid_blah.root")
    sys.exit(1)
dirPath = sys.argv[1]
fileId = sys.argv[2]
# cast the pattern string to bytes data type
fileId = fileId.encode("utf-8", errors="strict")

subDirs = os.listdir(dirPath)
print(f"Looking for file_id: {fileId} in {len(subDirs)} sub-directories under: {dirPath}")

for subDir in subDirs:
    newDir = os.path.join(dirPath, subDir)
    files = os.listdir(newDir)
    print(f"Found {len(files)} files under : {newDir}")
    for logArch in files:
        extractFile(os.path.join(newDir, logArch), fileId)
    print("\n\n")
sys.exit(0)
