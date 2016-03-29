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


getFilesAvailable = """
                    SELECT wsfa.fileid FROM wmbs_sub_files_available wsfa
                      LEFT OUTER JOIN wmbs_file_location wfl ON wsfa.fileid = wfl.fileid
                      WHERE wfl.fileid is null
                    """


getCERNLocation = """
                  SELECT location FROM wmbs_location_senames where se_name = 'T0_CH_CERN_Disk'
                  """


updateFileLocation = """
                     INSERT INTO wmbs_file_location (fileid, location) VALUES (:fileid, :location)
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
    myThread = threading.currentThread()
    formatter = DBFormatter(logging, myThread.dbi)

    # Get all the files available for each subscription
    print "Getting files available without location..."
    availFiles = formatter.formatDict(myThread.dbi.processData(getFilesAvailable))
    print "Total files available: %s" % len(availFiles)
    uniqAvailFiles = list(set([x['fileid'] for x in availFiles]))
    availFiles = [{'fileid': x} for x in uniqAvailFiles]
    print "Total unique files available: %s" % len(uniqAvailFiles)

    cernID = formatter.formatDict(myThread.dbi.processData(getCERNLocation))[0]
    print "CERN location id: %s" % cernID
    if not cernID:
        print "You need to add T0_CH_CERN to the resource control db"
        sys.exit(1)

    for fid in availFiles:
        fid.update(cernID)

    myThread.dbi.processData(updateFileLocation, availFiles)
    print "Done!"
    sys.exit(0)


if __name__ == '__main__':
    sys.exit(main())
