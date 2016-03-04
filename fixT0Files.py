import sys
import os
import threading
import logging
try:
    from WMCore.WMInit import connectToDB
    from WMCore.Database.DBFormatter import DBFormatter
except ImportError:
    print "You do not have a proper environment, please source the following:"
    print "source /data/srv/wmagent/current/apps/wmagent/etc/profile.d/init.sh"
    sys.exit(1)

getUnfinishedSubs = """
                    SELECT wmbs_subscription.id AS subId FROM wmbs_workflow 
                      INNER JOIN wmbs_subscription ON wmbs_workflow.id=wmbs_subscription.workflow
                      INNER JOIN wmbs_fileset ON wmbs_subscription.fileset=wmbs_fileset.id
                    WHERE wmbs_subscription.finished=0 and wmbs_workflow.name= :workflow
                    """

getFilesAvailable = """
                   SELECT wmbs_sub_files_available.fileid AS fileid FROM wmbs_sub_files_available
                     WHERE wmbs_sub_files_available.subscription = :subId
                   """

getFileLocation = """
                  SELECT wmbs_sub_files_available.fileid, wls.se_name AS pnn
                    FROM wmbs_sub_files_available
                    INNER JOIN wmbs_file_location ON
                      wmbs_sub_files_available.fileid = wmbs_file_location.fileid
                    INNER JOIN wmbs_location_senames wls ON
                      wmbs_file_location.location = wls.location
                  WHERE wmbs_sub_files_available.subscription = :subId
                  """


def main():
    """
    _main_
    """
    if 'WMAGENT_CONFIG' not in os.environ:
        os.environ['WMAGENT_CONFIG'] = '/data/srv/wmagent/current/config/wmagent/config.py'
    if 'manage' not in os.environ:
        os.environ['manage'] = '/data/srv/wmagent/current/config/wmagent/manage'

    args = sys.argv[1:]
    if not len(args) == 1:
        print "usage: python fixT0Files.py <text_file_with_the_workflow_names>"
        sys.exit(0)
    inputFile = args[0]
    with open(inputFile) as f:
        listWorkflows = [x.rstrip('\n') for x in f.readlines()]
    print listWorkflows
    print "WTF"
    connectToDB()
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)

    # Get all the unfinished subscriptions for a given workflow
    binds = [{'workflow': wf} for wf in listWorkflows]
    unfSubs = formatter.formatDict(myThread.dbi.processData(getUnfinishedSubs, binds))
    print "Unfinished subscriptions: %s" % unfSubs

    # Get all the files available for each subscription
    for sub in unfSubs:
        availFiles = formatter.formatDict(myThread.dbi.processData(getFilesAvailable, [sub]))
        print "Files available for sub %s: %s" % (sub, len(availFiles))

        availLocation = formatter.formatDict(myThread.dbi.processData(getFileLocation, [sub]))
        print "Files available with location for sub %s: %s" % (sub, len(availLocation))

        if len(availFiles) and not len(availLocation):
            # then we have to mark this file as failed
            _ = [x.update(sub) for x in availFiles]
            print "Failing the following: %s" % availFiles
            myThread.dbi.processData("INSERT INTO wmbs_sub_files_failed (subscription, fileid) VALUES (:subId, :fileid)", availFiles)
            myThread.dbi.processData("DELETE FROM wmbs_sub_files_available WHERE subscription = :subId AND fileid = :fileid", availFiles)
            myThread.dbi.processData("DELETE FROM wmbs_sub_files_acquired WHERE subscription = :subId AND fileid = :fileid", availFiles)
            myThread.dbi.processData("DELETE FROM wmbs_sub_files_complete WHERE subscription = :subId AND fileid = :fileid", availFiles)
            print "Subscription %s got %d files moved to failed" % (sub['subid'], len(availFiles)) 

    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
