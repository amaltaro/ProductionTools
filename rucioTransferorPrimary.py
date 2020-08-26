#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
WMCore prototype for MSTransferor primary input data placement with Rucio.
NOTE: this script can be safely executed, it does not create any Rucio rules!
"""
from __future__ import print_function, division
import sys
import os
import json
from pprint import pformat, pprint
from rucio.client import Client

SCOPE = "cms"
ACCT = "wma_prod"
### This container contains 3 blocks
DSET = "/ST_FCNC-TH_Thadronic_HToWWZZtautau_Ctcphi_CtcG_CP5_13TeV-mcatnlo-madspin-pythia8/RunIIFall17NanoAODv6-PU2017_12Apr2018_Nano25Oct2019_102X_mc2017_realistic_v7-v1/NANOAODSIM"
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
    Prototype for the rucio interactions to resolve a primary
    dataset within MSTransferor.
    The data structure expected is something like:
    {"container_name":
        {"block_name": {"blockSize": 111, "locations": ["x", "y"]},
         "block_name2": {"blockSize": 111, "locations": ["x", "y"]}, etc
    Based on this, we decide where rules need to be created and which blocks need
    to be transferred.
    :return:
    """
    print("Running MSTransferor prototype logic for primary input data placement...")
    print("Using:\n\tscope: {}\n\taccount: {}\n\tcontainer: {}\n".format(SCOPE, ACCT, DSET))

    ### STEP-1: provided a container name, find all its blocks and their sizes
    print("Finding out all the blocks present in the primary dataset: {}".format(DSET))
    primaryInfo = {DSET: {}}
    resp = client.list_dids(SCOPE, filters={'name': DSET}, long=True, recursive=True)
    for item in resp:
        if item['did_type'] == "DATASET":
            primaryInfo[DSET].setdefault(item['name'], {})
            primaryInfo[DSET][item['name']]['blockSize'] = item['bytes']
            primaryInfo[DSET][item['name']]['locations'] = []
            print("Found block: {}, with size: {}".format(item['name'], item['bytes']))

    ### STEP-2: now that we know all the block names, find their current location and
    ### where they have been locked with a rule created by our own account
    for block in primaryInfo[DSET]:
        resp = client.get_dataset_locks(SCOPE, block)
        for item in resp:
            if item['account'] == ACCT:
                print("Block: {}, is locked under RSE: {} by the rucio account: {}".format(block,
                                                                                           item['rse'],
                                                                                           item['account']))
                primaryInfo[DSET][item['name']]['locations'].append(item['rse'])
    print("\nAt this stage, we know the block names, their sizes, and where they are currently available and locked:")
    pprint(primaryInfo)

    ### STEP-3: compare blocks location with the current SiteWhitelist, and remove blocks already in place
    for block in list(primaryInfo[DSET]):
        commonLocation = set(SITE_WHITE_LIST) & set(primaryInfo[DSET][block]['locations'])
        if commonLocation:
            print("Dropping block: {} from data placement, it's already available at: {}".format(block, commonLocation))
            primaryInfo[DSET].pop(block)

    ### STEP-4: figure out the RSE quotas and make a rule against those sites in the white list (or a
    ### sub-set of it)
    kwargs = dict(grouping="DATASET", account=ACCT, comment="MSTransferor",
                  activity="MSTransferor Input Data Placement")
    dids = primaryInfo[DSET].keys()
    copies = 1
    rseExpr = "|".join(SITE_WHITE_LIST)
    # DATASET = replicates all files in the same block to the same RSE
    # FIXME: keeping the rule creation commented out
    resp = "fake_rule_id"
    #resp = client.add_replication_rule(dids, copies, rseExpr, **kwargs)
    print("Rule id: {} created for\n\tRSEs: {}\n\tDIDs: {}".format(resp, rseExpr, dids))


if __name__ == '__main__':
    sys.exit(main())
