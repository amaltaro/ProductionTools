#!/bin/sh
sandbox_dir=/data/srv/wmagent/current/install/wmagent/WorkQueueManager/cache
working_area=/data/srv/wmagent/current/sandbox_surgery_take2

### Change only these sandboxes, not ALL of those in the cache dir
sandboxes_list="prozober_recovery-1-fabozzi_Run2016C-v2-MET-07Aug17_8029__171011_213536_3906
prozober_recovery-1-fabozzi_Run2016D-v2-BTagCSV-07Aug17_8029__171011_154922_8907
prozober_recovery-1-fabozzi_Run2016D-v2-BTagMu-07Aug17_8029__171011_163150_622
prozober_recovery-1-fabozzi_Run2016D-v2-SinglePhoton-07Aug17_8029__171010_184014_2458
prozober_recovery-1-fabozzi_Run2016D-v2-Tau-07Aug17_8029__171011_212849_8912
prozober_recovery-1-fabozzi_Run2016E-v2-BTagCSV-07Aug17_8029__171011_162550_8392
prozober_recovery-1-fabozzi_Run2016E-v2-BTagMu-07Aug17_8029_171011_220212_5660
prozober_recovery-1-fabozzi_Run2016E-v2-MuonEG-07Aug17_8029__171011_214922_4650
prozober_recovery-2-fabozzi_Run2016H-v1-Tau-07Aug17_8029__171011_144811_459
prozober_recovery-2-fabozzi_Run2017B-v1-SingleElectron-12Sep2017_9211__171011_224409_581
prozober_recovery-2-fabozzi_Run2017C-v1-DisplacedJet-12Sep2017_9211_171011_220548_1773
prozober_recovery-2-fabozzi_Run2017C-v1-SingleElectron-12Sep2017_9211__171011_222655_6787
prozober_recovery-3-amaltaro_Run2016B-v2-DoubleEG-07Aug17_ver2_8029__171010_190811_3672
prozober_recovery-3-fabozzi_Run2016B-v2-MuOnia-07Aug17_ver2_8029__171011_175826_5804
prozober_recovery-3-fabozzi_Run2016B-v2-SinglePhoton-07Aug17_ver2_8029__171010_181531_3022
prozober_recovery-3-fabozzi_Run2016C-v2-MET-07Aug17_8029__171011_213555_7424
vlimant_recovery-2-fabozzi_Run2016B-v2-NoBPTX-07Aug17_ver2_8029__171012_121529_3224
vlimant_recovery-2-fabozzi_Run2016G-v1-DoubleEG-07Aug17_8029__171012_145244_7238
vlimant_recovery-2-fabozzi_Run2016G-v1-SinglePhoton-07Aug17_8029__171012_152049_7447
vlimant_recovery-3-fabozzi_Run2016G-v1-DoubleEG-07Aug17_8029__171012_145259_7612
vlimant_recovery-3-fabozzi_Run2016G-v1-SinglePhoton-07Aug17_8029__171012_152124_4847
vlimant_recovery-4-fabozzi_Run2016G-v1-DoubleEG-07Aug17_8029__171012_145415_4939
vlimant_recovery-4-fabozzi_Run2016G-v1-SinglePhoton-07Aug17_8029__171012_152212_5319
vlimant_recovery-5-fabozzi_Run2016G-v1-DoubleEG-07Aug17_8029__171012_145421_463
vlimant_recovery-5-fabozzi_Run2016G-v1-SinglePhoton-07Aug17_8029__171012_152219_9856"

# Backup location for all touched sandboxes
mkdir -p $working_area/archive

# Download the patch locally to avoid GH Too Many Requests
### FIXME patch number has to be updated manually
PATCH_NUMBER=8243
curl https://patch-diff.githubusercontent.com/raw/dmwm/WMCore/pull/${PATCH_NUMBER}.patch > ${working_area}/${PATCH_NUMBER}.patch

cd $working_area
### FIXME if you want to update ALL sandbox, then swap these 2 lines below
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
  echo "$sandbox taken care of!!!" && echo ""
done
echo "Good job!"
