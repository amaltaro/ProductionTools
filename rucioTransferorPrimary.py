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
DID_TYPE = "DATASET"
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
    print("Finding all the blocks present in the primary dataset: {}".format(DSET))
    primaryInfo = {DSET: {}}
    resp = client.list_dids(SCOPE, filters={'name': DSET + "#*", "type": DID_TYPE}, long=True)
    for item in resp:
        primaryInfo[DSET].setdefault(item['name'], {})
        primaryInfo[DSET][item['name']]['blockSize'] = item['bytes']
        primaryInfo[DSET][item['name']]['locations'] = []
        print("Found block: {}, with size: {}".format(item['name'], item['bytes']))

    ### STEP-2: if there is pileup dataset, then we need to intersect the SiteWhitelist
    ### with the current pileup container location (where the container is locked)
    ### otherwise, just make a rule for all the blocks to the workflow SiteWhitelist (grouping=DATASET)
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
