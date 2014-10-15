"""
__fixJobAccountant.py__

Fixes Report.pkl files when JobAccountant crashes reporting that
TaskName does not exist in the FJR.

Created on Oct 15, 2014.
@author: amaltaro
"""
import sys, os, subprocess
import threading
import logging
import time
from pprint import pprint
from optparse import OptionParser

try:
    from collections import defaultdict
    from WMCore.WMInit import connectToDB
    from WMCore.Database.DBFormatter import DBFormatter
    from WMCore.FwkJobReport.Report import Report
except ImportError:
    print "You do not have a proper environment, please source the following:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(1)

getQuery = """
            SELECT wj.fwjr_path, ww.task FROM wmbs_workflow ww
            INNER JOIN wmbs_subscription ws ON ws.workflow = ww.id
            INNER JOIN wmbs_jobgroup wjg ON wjg.subscription = ws.id
            INNER JOIN wmbs_job wj ON wj.jobgroup = wjg.id
            WHERE wj.id = """

def main():
    """
    _main_
    """
    usage = "Usage: %prog -j jobId"
    parser = OptionParser(usage = usage)
    parser.add_option('-j', '--jobId', help = 'Wmbs jobId reported in the component log', dest = 'jobId')
    (options, args) = parser.parse_args()
    if not options.jobId:
        parse.error('You must provide at least one jobId')
        print 'Example: python fixJobAccountant.py -j "1678 1679"'
        sys.exit(1) 
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    if 'manage' not in os.environ:
        os.environ['manage'] = '/data/srv/wmagent/current/config/wmagent/manage'

    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)

    for job in options.jobId.split(): 
        myQuery = getQuery + str(job)
        output = myThread.transaction.processData(myQuery)
        result = formatter.format(output)
        reportPath = result[0][0]
        taskName = result[0][1]
        #print 'Report path: %s' % reportPath
        #print 'Task name: %s' % taskName

        jr = Report(reportPath)
        if jr.getTaskName():
            print "Job id %s already has a TaskName %s.\nSkipping .." % (job, jr.getTaskName())
            continue
        jr.setTaskName(taskName)
        jr.save(reportPath)
        print "Updated TaskName for fwjr for jobId: %s" % job 

    print "Done!"
    return 0

if __name__ == '__main__':
    sys.exit(main())
