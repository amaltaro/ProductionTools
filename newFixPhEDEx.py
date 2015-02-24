"""
__newFixPhEDEx.py__

Query for all files in dbsbuffer_file table not injected in PhEDEx yet
(discarding unmerged and MCFakeFiles) and check whether they are already
know by PhEDEx or not.

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
           (lfn NOT LIKE '%%unmerged%%' AND lfn NOT LIKE 'MCFakeFile%%' AND lfn NOT LIKE '/store/user%%')
           """

setQuery = """
           UPDATE dbsbuffer_file SET in_phedex = 1 WHERE lfn = :lfn
           """

### TODO: organize this messs

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
    subprocess.call([os.environ['manage'], "execute-agent", "wmcoreD", "--shutdown",
                     "--component=PhEDExInjector"], stdout=open(os.devnull, 'wb'))
    time.sleep(5)

    ## TASK1: query DB for files not injected in phedex yet
    # Get the files that the PhEDExInjector would look for
    formatter = DBFormatter(logging, myThread.dbi)
    formatter.sql = getQuery
    results = formatter.execute()
    fileList = []
    fileList = [lfn[0] for lfn in results]

    ## TASK2: makes lfns a bit shorter to sort and uniq them
    reducedLfns = [lfn.rsplit('/',2)[0] for lfn in fileList]
    reducedLfns = list(set(reducedLfns))

    ## TASK3: build uniq dataset names and check whether PhEDEx and DBS contain
    ## the same number of files. If so, then those lfns are healthy
    print "Checking %d dataset in both PhEDEx and DBS ..." % len(reducedLfns)
    crippleLfns, healthyLfns = [], []
    i = 0
    n = len(reducedLfns)
    for lfn in reducedLfns:
        try:
            lfnAux = lfn.split ('/')
            dset = '/'+lfnAux[4]+'/'+lfnAux[3]+'-'+lfnAux[6]+'/'+lfnAux[5]
            result = myPhEDEx._getResult('blockreplicas', args = {'dataset' : dset}, verb = 'GET')
            phedexFiles = 0
            for item in result["phedex"]["block"]:
                phedexFiles += item['files']
    
            ## TODO: ValidFile is only available for > 0.9.95pre5. Once all agents are
            ## upgraded, then we can start using this new query.
            #result = myDBS.listDatasetFileDetails(dset)
            #dbsFiles = 0
            #for item in result.itervalues():
            #    dbsFiles += 1 if item['ValidFile'] else 0
    
            # This call returns valid+invalid number of filesfiles
            result = myDBS.listDatasetFiles(dset)
            dbsFiles = len(result)
            if phedexFiles == dbsFiles:
                healthyLfns.append(lfn)
            else:
                crippleLfns.append(lfn)
        except:
            print "Error with:",lfn
        i += 1
        if i % 100 == 0:
            print '%d/%d files processed'%(i,n) 
    ## TASK4: map the short cripple and healthy lists to the full original lfns
    ## TODO: this code looks terrible... IMPROVE IT!
    if crippleLfns:
        filesToCheck = []
        for lfn in crippleLfns:
            #filesToCheck = [file for file in fileList if lfn in file]
            for file in fileList:
                if lfn in file:
                    filesToCheck.append(file)
    else:
        filesToCheck = []
    if healthyLfns:
        filesInPhedex = []
        for lfn in healthyLfns:
            #filesInPhedex = [file for file in fileList if lfn in file]
            for file in fileList:
                if lfn in file:
                    filesInPhedex.append(file)
    else:
        filesInPhedex = []

    ## TASK5: query PhEDEx for each cripple file (filesToCheck)
    ## and build the final file lists
    missingFiles = []
    i = 0
    n = len(filesToCheck)
    for file in filesToCheck:
        try:
            result = myPhEDEx._getResult('data', args = {'file' : file}, verb = 'GET')
            if len(result['phedex']['dbs']):
                filesInPhedex.append(file)
            else:
                missingFiles.append(file)
        except:
            print "Error contacting Phedex", file
        i += 1
        if i % 100 == 0:
            print '%d/%d files processed'%(i,n)

    if not filesInPhedex:
        print "There are no files to be updated in the buffer. Contact a developer."
        print "Starting PhEDExInjector now ..."
        subprocess.call([os.environ['manage'], "execute-agent", "wmcoreD", "--start",
                     "--component=PhEDExInjector"], stdout=open(os.devnull, 'wb'))
        return 0
    print "Found %d out of %d files that are already registered in PhEDEx \
           but buffer doesn't know" % (len(filesInPhedex), len(fileList))
    print "Fixing them now, it may take several minutes ..."

    ## TASK6: time to actually fix these files
    binds = []
    for file in filesInPhedex:
        binds.append({'lfn': file})
    formatter.dbi.processData(setQuery, binds,
                              conn = None,
                              transaction = False,
                              returnCursor = False)

    print "Rows were successfully updated! Good job!"
    print "Starting PhEDExInjector now ..."
    subprocess.call([os.environ['manage'], "execute-agent", "wmcoreD", "--start",
                     "--component=PhEDExInjector"], stdout=open(os.devnull, 'wb'))
    print "Done!"
    return 0

if __name__ == '__main__':
    sys.exit(main())
