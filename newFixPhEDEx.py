"""
__newFixPhEDEx.py__

Queries for all files in dbsbuffer_file table not injected yet in PhEDEx
(discarding unmerged and MCFakeFiles) and check whether they need to be
injected or not.

Based on fixPhEDEx.py, created by dballest (thanks mate!).
Created on Aug 9, 2014.
@author: amaltaro
"""
import sys, os, subprocess
import threading
import logging
import time
from pprint import pprint

try:
    from collections import defaultdict
    from WMCore.WMInit import connectToDB
    from WMCore.Database.DBFormatter import DBFormatter
    from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
    from WMCore.Services.DBS.DBS3Reader import DBS3Reader
except ImportError:
    print "You do not have a proper environment, please source the following:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(1)

getQuery = """
           SELECT lfn FROM dbsbuffer_file WHERE in_phedex = 0 AND 
           (lfn NOT LIKE '%unmerged%' AND lfn NOT LIKE 'MCFakeFile%')
           """

setQuery = """
           UPDATE dbsbuffer_file set in_phedex = 1 where lfn = :lfn
           """

def main():
    """
    _main_
    """
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    if 'manage' not in os.environ:
        os.environ['manage'] = '/data/srv/wmagent/current/config/wmagent/manage'
    connectToDB()
    myPhEDEx = PhEDEx()
    myDBS = DBS3Reader('https://cmsweb.cern.ch/dbs/prod/global/DBSReader/')
    myThread = threading.currentThread()
    print "Shutting down PhEDExInjector..."
#    subprocess.call([os.environ['manage'], "execute-agent", "wmcoreD", "--shutdown",
#                     "--component=PhEDExInjector"], stdout=open(os.devnull, 'wb'))
    #time.sleep(5)

    # Get the files that the PhEDExInjector would look for
    formatter = DBFormatter(logging, myThread.dbi)
    formatter.sql = getQuery
    results = formatter.execute()
    fileList = []
    fileList = [lfn[0] for lfn in results]
    #fileList.sort()
    #print "fileLists: ", fileList
    print "fileLists: "
    pprint(fileList)
    reducedLfns = [lfn[0].rsplit('/',2)[0] for lfn in results]
    print "reducedLfns: "
    pprint(reducedLfns)
    reducedLfns = list(set(reducedLfns))
    print "reducedLfns uniq: "
    pprint(reducedLfns)
    print "Original list length: %d\tReduced list length: %d" % (len(fileList),len(reducedLfns))

    ### TODO: Now it's time to get rid of lfns that belong to datasets that
    ### have the same number of files in both DBS (valid only) and PhEDEx
    crippleLfns, healthyLfns = [], []
    for lfn in reducedLfns:
        lfnAux = lfn.split ('/')
        dset = '/'+lfnAux[4]+'/'+lfnAux[3]+'-'+lfnAux[6]+'/'+lfnAux[5]
        result = myPhEDEx._getResult('blockreplicas', args = {'dataset' : dset}, verb = 'GET')
        phedexFiles = 0
        for item in result["phedex"]["block"]:
            phedexFiles += item['files']

        # TODO: ValidFile is only available for > 0.9.95pre5
        #result = myDBS.listDatasetFileDetails(dset)
        #dbsFiles = 0
        #for item in result.itervalues():
        #    dbsFiles += 1 if item['ValidFile'] else 0

        # TODO: replace this call by the above one once we completely migrate
        # to WMA > 0.9.95b. For now it returns valid+invalid files
        result = myDBS.listDatasetFiles(dset)
        dbsFiles = len(result)
        print "Dataset: %s\tPhEDEx files: %d\tDBS files: %d" % (dset, phedexFiles, dbsFiles)

        if phedexFiles == dbsFiles:
            healthyLfns.append(lfn)
        else:
            crippleLfns.append(lfn)

    print "crippleLfns: "
    pprint(crippleLfns)
    print "healthyLfns: "
    pprint(healthyLfns)

    ### TODO: now we have to filter the original list with the cripple one
    if crippleLfns:
        filesToCheck = []
        for lfn in crippleLfns:
            #filesToCheck = [file for file in fileList if lfn in file]
            for file in fileList:
                if lfn in file:
                    filesToCheck.append(lfn)
        print "Bad files: "
        pprint(filesToCheck)
    if healthyLfns:
        for lfn in healthyLfns:
            #filesNotToCheck = [file for file in fileList if lfn in file]
            for file in fileList:
                if lfn in file:
                    filesNotToCheck.append(lfn)
        print "Good files: "
        pprint(filesNotToCheck)

    sys.exit(1)

    ### TODO: now we ask PhEDEx for the remaining lfns
    filesInPhedex = set()
    filesNotInPhedex = set()
    for lfn in fileList:
        result = myPhEDEx._getResult('data', args = {'file' : lfn}, verb = 'GET')
        if len(result['phedex']['dbs']):
            filesInPhedex.append(lfn)
        else:
            filesNotInPhedex.append(lfn)


    if not foundFiles:
        print "I didn't find an abnormal file, feel free to panic!. Please contact a developer."
        return 0
    print "Found %d files that are already registered in PhEDEx but the buffer doesn't know" % len(foundFiles)
    print "Fixing them now..."
    # Fix it!
    binds = []
    for lfn in foundFiles:
        binds.append({'lfn' :lfn})
    formatter.dbi.processData(modification, binds,
                                        conn = None,
                                        transaction = False,
                                        returnCursor = False)
    print "Fixed them! :)"
    print "You can restart the PhEDExInjector now, have a nice day!"
    return 0

if __name__ == '__main__':
    sys.exit(main())
