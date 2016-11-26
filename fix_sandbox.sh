#!/bin/sh
sandbox_dir=/data/srv/wmagent/current/install/wmagent/WorkQueueManager/cache
working_area=/data/srv/wmagent/current/sandbox_surgery

### Change only these sandboxes, not ALL of those in the cache dir
sandboxes_list="pdmvserv_task_BTV-RunIISummer15GS-00070__v1_T_161125_213537_5698 
pdmvserv_task_BTV-RunIISummer15GS-00071__v1_T_161125_213556_5980 
pdmvserv_task_BTV-RunIISummer15GS-00072__v1_T_161125_213611_6014
pdmvserv_task_BTV-RunIISummer15GS-00073__v1_T_161125_213646_6031
pdmvserv_task_BTV-RunIISummer15GS-00074__v1_T_161125_213652_5819
pdmvserv_task_SUS-RunIISummer15GS-00196__v1_T_161125_233700_7116"


# Backup location for all touched sandboxes
mkdir -p $working_area/archive

cd $working_area
#for sandbox in `ls $sandbox_dir`
for sandbox in $sandboxes_list
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
  wget -nv https://github.com/amaltaro/WMCore/commit/2fb0738893f84ef91086b43505447fe3a9ae511a.patch -O - | patch -p 3

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
  echo "$sandbox taken care of!!!" && echo ""
done
echo "Good job!"
