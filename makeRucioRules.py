#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to create standalone Rucio rules for input data, thus
using the wmcore_transferor Rucio account and pointing to Rucio
production instance.
It uses exactly the same parameters as those set by MSTransferor.

NOTE: it depends on the WMAgent environment to load the Rucio wrapper.
"""

import argparse
import logging

import sys
from WMCore.Services.Rucio.Rucio import Rucio

RUCIO_ACCT = "wmcore_transferor"
RUCIO_AUTH_URL = "https://cms-rucio-auth.cern.ch"
RUCIO_URL = "http://cms-rucio.cern.ch"


def parseArgs():
    """
    Well, parse the arguments passed in the command line :)
    """
    parser = argparse.ArgumentParser(description="Create Rucio container rules with wmcore_transferor acct")

    parser.add_argument('-c', '--container', required=True, help='Container name')
    parser.add_argument('-r', '--rse', required=True, help='RSE name or expression')
    args = parser.parse_args()
    return args


def loggerSetup(logLevel=logging.INFO):
    """
    Return a logger which writes everything to stdout.
    """
    logger = logging.getLogger(__name__)
    outHandler = logging.StreamHandler(sys.stdout)
    outHandler.setFormatter(logging.Formatter("%(asctime)s:%(levelname)s:%(module)s: %(message)s"))
    outHandler.setLevel(logLevel)
    logger.addHandler(outHandler)
    logger.setLevel(logLevel)
    return logger


if __name__ == '__main__':
    args = parseArgs()
    logger = loggerSetup()

    rucio = Rucio(acct=RUCIO_ACCT, hostUrl=RUCIO_URL, authUrl=RUCIO_AUTH_URL,
                  configDict={"logger": logger, "user_agent": "amaltaro/makeRucioRules"})
    rule = {'copies': 1,
            'activity': 'Production Input',
            'lifetime': None,
            'account': RUCIO_ACCT,
            'grouping': "ALL",
            'comment': 'WMCore MSTransferor input data placement'}
    logger.info("\nCreating rule for DID: %s, with RSE: %s and other attrs: %s",
                args.container, args.rse, rule)
    resp = rucio.createReplicationRule(args.container, args.rse, **rule)
    logger.info("Response: %s", resp)
