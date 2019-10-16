from __future__ import print_function

from WMCore.WorkQueue.WorkQueue import globalQueue

### Same configuration as defined in the deployment scripts
BASE_URL = "https://cmsweb.cern.ch"

COUCH_URL = "%s/couchdb" % BASE_URL
REQMGR2 = "%s/reqmgr2" % BASE_URL
WEBURL = "%s/%s" % (COUCH_URL, "workqueue")
LOG_DB_URL = "%s/wmstats_logdb" % COUCH_URL
LOG_REPORTER = "global_workqueue"

queueParams = {}
queueParams['CouchUrl'] = COUCH_URL
queueParams['DbName'] = "workqueue"
queueParams['InboxDbName'] = "workqueue_inbox"
queueParams['WMStatsCouchUrl'] = "%s/%s" % (COUCH_URL, "wmstats")
queueParams['QueueURL'] = WEBURL
queueParams['ReqMgrServiceURL'] = REQMGR2
queueParams['RequestDBURL'] = "%s/%s" % (COUCH_URL, "reqmgr_workload_cache")
queueParams['central_logdb_url'] = LOG_DB_URL
queueParams['log_reporter'] = LOG_REPORTER
queueParams['rucioAccount'] = ""

globalQ = globalQueue(**queueParams)
globalQ.updateLocationInfo()
