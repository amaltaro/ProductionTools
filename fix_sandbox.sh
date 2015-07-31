#!/bin/sh

# Apply the xrdcp patch
wget -nv https://github.com/dmwm/WMCore/pull/6085.patch -O - | patch -d apps/wmagent/lib/python2.6/site-packages/ -p 3
 
sandbox_dir=/data/srv/wmagent/current/install/wmagent/WorkQueueManager/cache/
working_area=/data/srv/wmagent/current/surgery_sandbox

# Backup location for all sandboxes
mkdir -p $working_area/archive

# Get the updated plugin
cd $working_area
curl https://raw.githubusercontent.com/lucacopa/WMCore/4e873898282748b7ee99c7687e9246246a32feae/src/python/WMCore/Storage/Backends/FNALImpl.py > FNALImpl.py

for sandbox in `ls $sandbox_dir`
do
  # Our scratch area
  mkdir temp
  cd temp

  # Copy sandbox and fix it
  cp $sandbox_dir/$sandbox/$sandbox-Sandbox.tar.bz2 .
  tar -xjf $sandbox-Sandbox.tar.bz2
  unzip -q WMCore.zip
  cp $working_area/FNALImpl.py WMCore/Storage/Backends/
  rm WMCore.zip
  zip -rq WMCore.zip WMCore
  rm -rf WMCore/

  #Move former sandbox to archive and create the new one
  mv $sandbox-Sandbox.tar.bz2 $working_area/archive
  tar -cjf $sandbox-Sandbox.tar.bz2 ./*

  #Copy the fixed Sandbox to the original area
  cp $sandbox-Sandbox.tar.bz2 $sandbox_dir/$sandbox/
 
  # clean our scratch area
  cd $working_area
  rm -rf temp/
done
echo "Good job!"
