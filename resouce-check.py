#!/usr/bin/env python
#pylint: disable-msg=C0103
"""
resource-check

Utility script for manipulating resource control.
"""
import os
from pprint import pprint

from WMCore.WMInit import connectToDB
from WMCore.Configuration import loadConfigurationFile
from WMCore.ResourceControl.ResourceControl import ResourceControl


connectToDB()
wmConfig = loadConfigurationFile(os.environ['WMAGENT_CONFIG'])
myResourceControl = ResourceControl(config = wmConfig)
pprint(myResourceControl.listThresholdsForCreate())
pprint(myResourceControl.listThresholdsForSubmit())
