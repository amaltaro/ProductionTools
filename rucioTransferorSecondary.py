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
from random import choice
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
    resp = client.list_dids(SCOPE, filters={'name': DSET}, long=True, recursive=True)
    for item in resp:
        if item['did_type'] == "DATASET":
            secondaryInfo[DSET]["bytes"] += item['bytes']
            secondaryInfo[DSET]["blocks"].append(item['name'])
    print("Dataset: {} has bytes: {}\n".format(DSET, secondaryInfo[DSET]["bytes"]))

    ### STEP-2: first, check whether there are container level rules that our
    ### own account might have already created (this can save a bunch of other Rucio calls!)
    secondaryRules = []
    resp = client.list_did_rules(SCOPE, DSET)
    for item in resp:
        if item['account'] == ACCT and item['grouping'] == "ALL":
            secondaryRules.append(item)
    ### now figure out the RSE expression and try to pin point things to a specific location
    if not secondaryRules:
        print("There are no rules for container: {}, account: {}, grouping=ALL".format(DSET, ACCT))
    for rule in secondaryRules:
        resp = client.list_rses(rule['rse_expression'])
        rses = [item['rse'] for item in resp]
        ### FIXME: does it make sense?
        if rule['copies'] == len(rses):
            msg = "Rule id: {}, account: {}, grouping: {}, copies: {} has the\n\tdataset: {}"
            msg += "\n\tlocked at RSEs: {}"
            print(msg.format(rule['id'], rule['account'], rule['grouping'], rule['copies'], rule['name'], rses))
            # remove that dataset from the input data placement
            secondaryInfo.pop(rule['name'])
            break
    if not secondaryInfo:
        print("There are no pileup container to be transferred! Exiting...")
        sys.exit(0)

    ### STEP-3: figure out the final container location (common location between all the blocks)
    # FIXME: pretty inefficient!!! We will likely have to cache pileup container location for a few hours, and
    # of course, keep it updated with whatever write actions we take
    blocksRSEs = []
    for block in secondaryInfo[DSET]["blocks"]:
        resp = client.get_dataset_locks(SCOPE, block)
        rses = set()
        for item in resp:
            if item['account'] == ACCT:
                rses.add(item['rse'])
        print("Block: {}, is locked under RSE: {} by the rucio account: {}".format(block,
                                                                                   rses,
                                                                                   ACCT))
        blocksRSEs.append(rses)
    ### Check the final container location
    ### FIXME: we likely need to have a container presence by RSE metric, such that we can make
    ### an intelliggent data placement (easy to count that as block numbers, but better with block sizes)
    finalContainerRSEs = blocksRSEs[0] if blocksRSEs else set()
    for blockRSE in blocksRSEs:
        finalContainerRSEs = finalContainerRSEs & blockRSE
    secondaryInfo[DSET]['locations'] = finalContainerRSEs
    ### FIXME: list of blocks is no longer needed here, release memory
    secondaryInfo[DSET].pop("blocks")

    ### STEP-4: compare containers location with the current SiteWhitelist, and remove containers already in place
    for container in list(secondaryInfo):
        commonLocation = set(SITE_WHITE_LIST) & set(secondaryInfo[DSET]['locations'])
        if commonLocation:
            print("Dropping container: {} from data placement, it's already available at: {}".format(block, commonLocation))
            secondaryInfo.pop(container)

    ### STEP-5: figure out the RSE quotas and make a rule against the best site (single copy)
    kwargs = dict(grouping="ALL", account=ACCT, comment="MSTransferor",
                  activity="MSTransferor Input Data Placement")
    for container in secondaryInfo:
        copies = 1
        rseExpr = choice(SITE_WHITE_LIST)
        # FIXME: keeping the rule creation commented out
        resp = "fake_rule_id"
        #resp = client.add_replication_rule(container, copies, rseExpr, **kwargs)
        print("\nRule id: {} created for\n\tRSEs: {}\n\tDID: {}".format(resp, rseExpr, container))


if __name__ == '__main__':
    sys.exit(main())
