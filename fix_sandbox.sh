#!/bin/sh
sandbox_dir=/data/srv/wmagent/current/install/wmagent/WorkQueueManager/cache
working_area=/data/srv/wmagent/current/surgery_10112

### Change only these sandboxes, not ALL of those in the cache dir
sandboxes_list="prozober_recovery-1-fabozzi_Run2016C-v2-MET-07Aug17_8029__171011_213536_3906
prozober_recovery-1-fabozzi_Run2016D-v2-BTagCSV-07Aug17_8029__171011_154922_8907"

# Backup location for all touched sandboxes
mkdir -p $working_area/archive

# Download the patch locally to avoid GH Too Many Requests
### FIXME patch number has to be updated manually
PATCH_NUMBER=10112
curl https://patch-diff.githubusercontent.com/raw/dmwm/WMCore/pull/${PATCH_NUMBER}.patch > ${working_area}/${PATCH_NUMBER}.patch

cd $working_area
### FIXME if you want to update ALL sandbox, then swap these 2 lines below
#for sandbox in $sandboxes_list
for sandbox in `ls $sandbox_dir`
do
  # does this file exist at all? If not, just skip to the next one
  if ! [ -e "${sandbox_dir}/${sandbox}/${sandbox}-Sandbox.tar.bz2" ];
  then
    echo "Counldn't find this sandbox: ${sandbox_dir}/${sandbox}/${sandbox}-Sandbox.tar.bz2" && echo ""
    continue
  fi

  # Our scratch area
  mkdir temp
  cd temp

  # Copy sandbox and fix it
  cp $sandbox_dir/$sandbox/$sandbox-Sandbox.tar.bz2 .
  tar -xjf $sandbox-Sandbox.tar.bz2
  unzip -q WMCore.zip
  # Do the dirty job; Apply the SetupCMSSW tweak
  cat ${working_area}/${PATCH_NUMBER}.patch | patch -p 3

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
  echo "$sandbox successfully patched!!!" && echo ""
done
echo "Good job!"
