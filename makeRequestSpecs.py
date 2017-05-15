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


def furtherTweaks(spec, reqType=None):
    reqmgrArgs = ('Requestor', 'RequestorDN', 'RequestName', 'RequestStatus', 'RequestTransition',
                  'RequestDate', 'CouchURL', 'CouchDBName', 'CouchWorkloadDBName')

    # snipe in a few floating arguments
    if reqType == 'TaskChain':
        spec['Task1'] = {"default": {}, "optional": False, "type": dict}
    elif reqType == 'StepChain':
        spec['Step1'] = {"default": {}, "optional": False, "type": dict}
    elif reqType == 'ReReco':
        # then update the Skim key names with 1 in the end to avoid confusion
        for k in spec:
            newkey = k.replace('#N', '1')
            val = spec.pop(k)
            spec[newkey] = val
            # sigh ... these Skim arguments are not really clear...
            if newkey.startswith('Skim'):
                spec[newkey]['optional'] = True

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
        # hack ReReco floating args in
        if req == 'ReReco':
            reqSpec.update(spec.getSkimArguments())
        reqSpec = furtherTweaks(reqSpec, req)

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
