#!/bin/sh

HOST=`hostname`
DATENOW=`date +%s`
LASTCHANGE=`stat -c %Y /data/srv/wmagent/current/install/wmagent/ErrorHandler/ComponentLog`
INTERVAL=`expr $DATENOW - $LASTCHANGE`
if (("$INTERVAL" >= 900)); then
  . /data/admin/wmagent/env.sh
  /data/srv/wmagent/current/config/wmagent/manage execute-agent wmcoreD --restart --components=ErrorHandler
  echo "cronjob in $HOST" | mail -s "ErrorHandler restarted" alan.malta@cern.ch,sryu@fnal.gov
fi
