# cmst0@cern.ch
"""
Used by the T0 team to change the MaxRSS requirements for
Merge tasks in a non-easy way, since we don't have any
APIs to change Merge job requirements
"""

from __future__ import print_function

import pickle
import sys

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


def check_current_values(workHelper):
    # check current values
    print('**********************************************************')
    for taskPath in workHelper.listAllTaskPathNames():
        task = workHelper.getTaskByPath(taskPath)
        print("Task: %s has CMSSW: %s" % (task.name(), task.getSwVersion()))
    print('**********************************************************')


pkl_file = str(sys.argv[1])
newMaxRSS = str(sys.argv[2])
outputWMWorkloadName = 'WMWorkload'

with open(pkl_file, "r") as configHandle:
    worker = pickle.load(configHandle)

# Prints a text version of the original pkl file for verification purposes
with open('originalPKL.txt', 'w') as oldTextPklHandler:
    oldTextPklHandler.write(str(worker))

workHelper = WMWorkloadHelper(worker)

print("PKL File:" + str(pkl_file))
for task in workHelper.getAllTasks():
    print("Modifying: " + task.getPathName())
    print("TASK")
    # print (type(task._propMethodMap()['MaxRSS']))
    # print (dir(task))
    print("BEFORE")
    print(task.data.section_("watchdog"))
    # task.setMaxRSS(12000000)
    task.setMaxRSS(newMaxRSS)
    print("AFTER")
    print((task.data.section_("watchdog")))  # Pkl the modified object

    if task.taskType() == "Merge":
        print("Are you sure you want to update this Merge task? I think so...")
        # do it the hard way
        task._setPerformanceMonitorConfig()
        task.monitoring.PerformanceMonitor.maxRSS = int(newMaxRSS)

with open(pkl_file, 'wb') as pf:
    pickle.dump(worker, pf)

# Prints a text version of the pkled file for verifitacion purposes
with open(pkl_file, 'r') as newPklHandler:
    loadedNewPkl = pickle.load(newPklHandler)

with open('modifiedPKL.txt', 'w') as newTextPklHandler:
    newTextPklHandler.write(str(loadedNewPkl))
