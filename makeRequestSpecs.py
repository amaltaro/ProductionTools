#!/bin/usr/env python
"""
Load each WMSpec file and create a json file with the create arguments
definition
"""
from __future__ import print_function
import sys
from pprint import pprint

from WMCore.WMFactory import WMFactory


def getWorkloadFactory(requestType):
    pluginFactory = WMFactory("specArgs", "WMSpec.StdSpecs")
    alteredClassName = "%sWorkloadFactory" % requestType
    spec = pluginFactory.loadObject(classname=requestType, alteredClassName=alteredClassName)
    return spec


def furtherTweaks(spec):
    reqmgrArgs = ('Requestor', 'RequestorDN', 'RequestName', 'RequestStatus', 'RequestTransition',
                  'RequestDate', 'CouchURL', 'CouchDBName', 'CouchWorkloadDBName')

    # first remove arguments that are set by ReqMgr2
    specArgs = {k: v for k, v in spec.items() if k not in reqmgrArgs}
    # then remove a couple of definitions not important for this purpose
    for key, defin in specArgs.iteritems():
        defin.pop('assign_optional', None)
        defin.pop('attr', None)
        defin.pop('validate', None)
        defin.pop('null', None)
    return specArgs


def writeToFile(fileName, data):
    print("Creating file: %s" % fileName)
    with open(fileName, 'w') as fo:
        pprint(data, fo, width=160)
    return


def main():
    requestTypes = ('DQMHarvest', 'MonteCarlo', 'MonteCarloFromGEN', 'ReDigi',
                    'ReReco', 'StepChain', 'TaskChain', 'StoreResults')
    for req in requestTypes:
        spec = getWorkloadFactory(req)
        reqSpec = spec.getWorkloadCreateArgs()
        reqSpec = furtherTweaks(reqSpec)

        reqFile = '%s_createSpec.json' % req
        writeToFile(reqFile, reqSpec)

        # also fetch the Task and Step specification (both for generator and processing chain)
        if req in ('StepChain', 'TaskChain'):
            # firstTask and generator
            reqSpec = spec.getChainCreateArgs(True, True)
            reqSpec = furtherTweaks(reqSpec)
            reqFile = '%s_create_%s_GeneratorSpec.json' % (req, req[:4])
            writeToFile(reqFile, reqSpec)

            # not first task and not a generator
            reqSpec = spec.getChainCreateArgs()
            reqSpec = furtherTweaks(reqSpec)
            reqFile = '%s_create_%s_ProcessingSpec.json' % (req, req[:4])
            writeToFile(reqFile, reqSpec)
    print("All fine!")


if __name__ == "__main__":
    sys.exit(main())
