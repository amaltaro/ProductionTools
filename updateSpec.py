"""
Script used to fix the backward incompatibility feature added in #7998

Given a list of request names, fetch the spec file of each of them and add
a new attribute (input.dataTier) to all Merge and Cleanup tasks. Then save
the new spec file again in couchdb.
"""
from __future__ import print_function

import sys

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

HELPER = WMWorkloadHelper()


def main():
    if len(sys.argv) != 2:
        print("Usage: python updateSpec.py <input_file_with_request_names>")
        print(" e.g.: python updateSpec.py workflows.txt")
        sys.exit(0)

    inputFile = sys.argv[1]
    with open(inputFile) as fo:
        listWfs = fo.read().splitlines()

    for wf in listWfs:
        updateSpec(wf)

    sys.exit(0)


def updateSpec(requestName):
    """
    Given a request name, fetch its spec file from cmsweb couch and
    parse every single task and add one attribute to the input reference
    section
    """
    reqUrl = "https://cmsweb-testbed.cern.ch/couchdb/reqmgr_workload_cache"
    HELPER.loadSpecFromCouch(reqUrl, requestName)
    if HELPER.getRequestType() == "StepChain":
        print("Skipping spec changes for StepChain %s" % requestName)
        return

    saveChanges = False
    print("Changing spec for %s" % requestName)
    for task in HELPER.taskIterator():
        print("    Top level task: %s" % task.name())
        for tt in task.taskIterator():
            print("    Iterating over task: %s" % tt.name())
            if tt.taskType() in ["Production", "Processing", "Skim"]:
                res = tt.getOutputModulesForTask(cmsRunOnly=True)
                if len(res) > 1:
                    print("    ERROR: there should not exist > 1 output module!!!")
                    return
                parentOutMod = res[0].dictionary_()
                print("    Parent out mods: %s" % parentOutMod.keys())
            elif tt.taskType() in ["Merge", "Cleanup"]:
                if hasattr(tt.data.input, "outputModule"):
                    inputOutMod = tt.data.input.outputModule
                    inputOutTier = parentOutMod[inputOutMod].dataTier
                    print("        input.outputModule: %s with tier: %s" % (inputOutMod, inputOutTier))
                    if not hasattr(tt.data.input, "dataTier") and inputOutTier:
                        # then set this attribute
                        setattr(tt.data.input, "dataTier", inputOutTier)
                        saveChanges = True

    if saveChanges:
        print("    Saving changed spec file for: %s\n" % requestName)
        HELPER.saveCouchUrl(HELPER.specUrl())

    return


if __name__ == '__main__':
    main()
