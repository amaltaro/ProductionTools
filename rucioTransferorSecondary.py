#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
WMCore prototype for MSTransferor secondary input data placement with Rucio
NOTE: this script can be safely executed, it does not create any Rucio rules!
"""
from __future__ import print_function, division
import sys
import os
import json
import datetime
from random import choice
from rucio.client import Client

SCOPE = "cms"
ACCT = "wma_prod"
DID_TYPE = "DATASET"
DSET = "/Neutrino_E-10_gun/RunIISummer19ULPrePremix-UL18_106X_upgrade2018_realistic_v11_L1v1-v2/PREMIX"
DSET2 = "/ST_FCNC-TH_Thadronic_HToWWZZtautau_Ctcphi_CtcG_CP5_13TeV-mcatnlo-madspin-pythia8/RunIIFall17NanoAODv6-PU2017_12Apr2018_Nano25Oct2019_102X_mc2017_realistic_v7-v1/NANOAODSIM"
STUCK_LIMIT = 7  # 7 days
SITE_WHITE_LIST = ["T2_CH_CERN", "T1_US_FNAL"]

if not os.getenv("X509_USER_CERT") or not os.getenv("X509_USER_KEY"):
    print("ERROR: you need to export the X509 environment variables")
    sys.exit(1)
CREDS = {"client_cert": os.getenv("X509_USER_CERT"),
         "client_key": os.getenv("X509_USER_KEY")}

client = Client(rucio_host='http://cms-rucio.cern.ch',
                auth_host='https://cms-rucio-auth.cern.ch',
                account=ACCT,
                ca_cert=False,
                auth_type="x509",
                timeout=30,
                creds=CREDS)

def main():
    """
    Prototype for the rucio interactions to resolve a secondary
    dataset within MSTransferor.
    The data structure expected is something like:
    {"container_name":
        {"bytes": 111, "locations": ["x", "y"], "blocks": []},
        {"bytes": 111, "locations": ["x", "y"], "blocks": []},
    Based on this, we decide where rules need to be created and which containers need
    to be transferred.
    :return:
    """
    print("Running MSTransferor prototype logic for secondary input data placement...")
    print("Using:\n\tscope: {}\n\taccount: {}\n\tcontainer: {}\n".format(SCOPE, ACCT, DSET))

    ### STEP-1: provided a secondary container name, find its total size
    ### we also have to store all its blocks to find out the final container location
    print("Calculating the total secondary size...")
    secondaryInfo = {DSET: {"bytes": 0, "locations": [], "blocks": []}}
    resp = client.list_dids(SCOPE, filters={'name': DSET + "#*", "type": DID_TYPE}, long=True)
    for item in resp:
        secondaryInfo[DSET]["bytes"] += item['bytes']
        secondaryInfo[DSET]["blocks"].append(item['name'])
    print("Dataset: {} has bytes: {}\n".format(DSET, secondaryInfo[DSET]["bytes"]))

    ### STEP-2: first, check whether there are container level rules that our
    ### own account might have already created (this can save a bunch of other Rucio calls!)
    secondaryRules = []
    resp = client.list_replication_rules(filters={"scope": SCOPE, "name": DSET, "account": ACCT, "grouping": "A"})
    dateTimeNow = datetime.datetime.now()
    for item in resp:
        ### FIXME: should we bother about any other state?
        if item['state'] == "SUSPENDED":
            print("WARNING: Dataset: {} has a SUSPENDED rule. Rule info: {}".format(DSET, item))
            continue
        elif item['state'] == "STUCK":
            stuckAt = item['stuck_at']
            timeDiff = dateTimeNow - stuckAt
            if int(timeDiff.days) > STUCK_LIMIT:
                msg = "WARNING: Dataset: {} has a STUCK rule for longer than {} days.".format(DSET, timeDiff.days)
                msg += " Not going to use it! Rule info: {}".format(item)
                print(msg)
                continue
            else:
                msg = "WARNING: Dataset: {} has a STUCK rule for only {} days.".format(DSET, timeDiff.days)
                msg += " Considering it for the pileup location"
                print(msg)
        ### NOTE that MSTransferor will only make container-level data placement for pileups
        ### and those ALWAYS target a single RSE, so the expression here should actually
        ### be the final RSE where data is
        secondaryInfo[DSET]["locations"].append(item['rse_expression'])
        secondaryRules.append(item)

    ### STEP-3: check whether container location/lock and SiteWhitelist have any RSEs in common
    ### triggering a new rule creation or not
    if not secondaryInfo[DSET]["locations"]:
        print("Pileup {} is not available anywhere. A container-level rule will have to be created".format(DSET))
    else:
        commonLocation = set(SITE_WHITE_LIST) & set(secondaryInfo[DSET]["locations"])
        if commonLocation:
            msg = "Pileup {} has a common location with the workflow SiteWhitelist.".format(DSET)
            msg += " No need to create another rule! Common RSEs are: {}".format(commonLocation)
            print(msg)
            sys.exit(0)
        else:
            msg = "Pileup {} has current locations: {}, while the SiteWhitelist is: {}".format(DSET,
                                                                                               secondaryInfo[DSET]["locations"],
                                                                                               SITE_WHITE_LIST)
            msg += " It has no common locations and a new rule needs to be created. "
            msg += "Or if it's a PREMIX pileup, this workflow should actually fail assignment unless AAA is enabled"
            print(msg)

    ### STEP-4: using Eric's function to select the best RSE within the SiteWhitelist (and primary locations)
    ### create a new container-lelve rule for this pileup
    kwargs = dict(grouping="ALL", account=ACCT, comment="MSTransferor Pileup",
                  activity="MSTransferor Input Data Placement")
    for container in secondaryInfo:
        copies = 1
        ### FIXME TODO Use Erics function to find out which RSE to use
        rseExpr = choice(SITE_WHITE_LIST)
        # FIXME: keeping the rule creation commented out
        resp = "fake_rule_id"
        # resp = client.add_replication_rule(container, copies, rseExpr, **kwargs)
        print("\nRule id: {} created for\n\tRSEs: {}\n\tDID: {}".format(resp, rseExpr, container))


if __name__ == '__main__':
    sys.exit(main())
