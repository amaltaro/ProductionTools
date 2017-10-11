"""
Script used to fix the backward incompatibility feature added in #7998

Given a list of request names, fetch the spec file of each of them and add
a new attribute (input.dataTier) to all Merge and Cleanup tasks. Then save
the new spec file again in couchdb.
"""
from __future__ import print_function

import sys
from pprint import pformat
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
    saveChanges = False
    reqUrl = "https://cmsweb.cern.ch/couchdb/reqmgr_workload_cache"
    HELPER.loadSpecFromCouch(reqUrl, requestName)
    print("SPEC: %s" % requestName)

    if HELPER.getRequestType() == "StepChain":
        print("SKIP spec changes for StepChain %s" % requestName)
        return
    # elif HELPER.getRequestType() == "Resubmission":  # see issue 8239
    elif HELPER.data.request.schema.RequestType == "Resubmission":
        if len(HELPER.getTopLevelTask()) > 1:
            print("ERROR: How come it has more than one top level task... abort abort!")
            return

        tt = HELPER.getTopLevelTask()[0]
        if tt.taskType() == "Merge":
            print("    Top level Merge task: %s" % tt.name())
            if hasattr(tt.data.input, "outputModule"):
                inputOutMod = tt.data.input.outputModule
                if not hasattr(tt.data.input, "dataTier"):
                    # be safe, load the Merged outputModule instead of relying on the parent
                    res = tt.getOutputModulesForTask(cmsRunOnly=True)
                    if len(res) > 1:
                        print("    ERROR: there should not exist > 1 output module!!!")
                        return
                    inputOutTier = res[0].Merged.dataTier
                    print("        updating input.outputModule: %s with input.dataTier: %s" % (inputOutMod, inputOutTier))
                    setattr(tt.data.input, "dataTier", inputOutTier)
                    saveChanges = True

    for task in HELPER.getAllTasks():
        if task.taskType() in ["Production", "Processing", "Skim"]:
            print("    Parent task: %s" % task.name())
            res = task.getOutputModulesForTask(cmsRunOnly=True)
            if len(res) > 1:
                print("    ERROR: there should not exist > 1 output module!!!")
                return
            parentOutMod = res[0].dictionary_()
            print("    Parent out mods: %s" % parentOutMod.keys())

            for tt in task.childTaskIterator():
                if tt.taskType() == "Merge":
                    print("    Child Merge task: %s" % tt.name())
                    if hasattr(tt.data.input, "outputModule"):
                        inputOutMod = tt.data.input.outputModule
                        if not hasattr(tt.data.input, "dataTier"):
                            # be safe, load the Merged outputModule instead of relying on the parent
                            res = tt.getOutputModulesForTask(cmsRunOnly=True)
                            if len(res) > 1:
                                print("    ERROR: there should not exist > 1 output module!!!")
                                return
                            inputOutTier = res[0].Merged.dataTier
                            print("        updating input.outputModule: %s with input.dataTier: %s" % (inputOutMod, inputOutTier))
                            setattr(tt.data.input, "dataTier", inputOutTier)
                            saveChanges = True
                elif tt.taskType() == "Cleanup":
                    print("    Child Cleanup task: %s" % tt.name())
                    if hasattr(tt.data.input, "outputModule"):
                        inputOutMod = tt.data.input.outputModule
                        inputOutTier = parentOutMod[inputOutMod].dataTier
                        if not hasattr(tt.data.input, "dataTier") and inputOutTier:
                            # then set this attribute based on the parent settings
                            print("        updating input.outputModule: %s with input.dataTier: %s" % (inputOutMod, inputOutTier))
                            setattr(tt.data.input, "dataTier", inputOutTier)
                            saveChanges = True

    if saveChanges:
        print("    Saving changed spec file for: %s\n" % requestName)
        HELPER.saveCouchUrl(HELPER.specUrl())

    return


if __name__ == '__main__':
    main()
