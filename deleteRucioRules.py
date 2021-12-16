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
#RUCIO_AUTH_URL = "https://cms-rucio-auth.cern.ch"
#RUCIO_URL = "http://cms-rucio.cern.ch"
RUCIO_AUTH_URL = "https://cmsrucio-auth-int.cern.ch"
RUCIO_URL = "http://cmsrucio-int.cern.ch"


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

    rules = ["3f0aa87dd50242008c6fe0d7a7a2b6a7", "8a414453fddc444897132d025bd26713", "c4489f11c18349169ad39df96de9f762", "0b7e1b910a2c4d12a46a85d9a84eb675", "2c8cc4d8f87b49059a18c7d9159a2764", "6ca3f4db2cfa4c84b55b3aa9d8f97aea", "b6bcaace0c9942c6a6e2356ec5c275fe", "4e07236eda394a1db97d0583a0c3a251", "41c2eeef2b24475ca4d64051ab769dbd", "03ec5b6e81a0465298e2a01f988f7c3e", "e02c143707344957a2d4da595f389531", "71f263adbc5d4c1bbd992ec20fd21319", "fba76b98e5f94c8c95cbf598d902a5ae", "a815fcc34f914c28afb64bc5254672a9", "a6c6072fe15b46788062db6a2d74ef3e", "9a88863b37f640109b0680080a7858a9", "457f017ce4554d6eb021dbf949bea541", "264d277d72c546f0b565f7627a8e159c", "22df2b5c214e4acf823f6f4db7c42dea", "b449e6f086024c2690b0c54140b26a34", "d5f223d958b04385805280d718806fe1", "57a5237e62a34829815041e64b19faf2", "632425e4be654fa284616f603fb1e192", "a570bdb3c6d747b6949b9f2a4df2819d", "06b1a8d0080046598af1b5a0e453fac8", "a04c79af550d40d88071d9ad29800822", "f2c233e1b55c4f6fb1a9230db8878a73", "d6925f1b03834396af05951a9956faa2", "bb74a4eaf3c04462b78a82f2b2d4316e", "f9b2d49e5f9e4513a17be7b86ce5b918", "86b4960117d94e65b2ec7d1fc5b50e9a", "ba3daf5d62db44dabf355aa7a251ad10", "e558aec8620143e49443a0ab6990dbf9", "b0efb49af8084c18ac878a12285159d8", "80a7026de2f342c286a9c57c02eb96bf", "7f6d2837f167466db69b4278fc42be37", "547f3d62480d4dbdb77a7203a5d42fb2", "ec1eba882b2d4fbd8503c8393dc57541", "4cbfe213933a41ebb4e8aca7c97e57ed", "2be860bd213947c9a6584ceacbf83736", "06ae7a700c5e4773a8b77873a5446800", "9838150557c14887928e64cfda6436df", "80011f524b264aa89ea8523e493c4b83", "5bbe9ad18b65494481dd6c79c282a8f8", "59a01b32506b457ba95156559e416d8a", "9631f8616afc4a9581dc0f027239a1ce", "79bd7ce876cc49bab1114f3b4927de25", "d9d8b134b0d2448795faed747304f893", "43805111bfa249818660ef6ca17aaf5c", "4becc1c49f99454cb9342b080a6bf609", "c529ae4f92494bfe98d365e2ae6166f0", "4aed544e7492475eb55c37b5f8e7835b", "ba745af4b203453b87d81e4bb601a26c", "e1dea040e00a4af58a0d37fb8ad0ac00", "d42193185a8e4f888248a5a1c03090d9", "357e2a441d3f4109b59af55f7b6540ac", "aeddb58180104476ba358da3bb076a1d", "671302ca27c44b9eb7f2c260c3af9da8", "e74791fe65a54d56813ebc41b37c3ae4", "f153046617cb4e95969a6a6926674a25", "baa88e69641a413f8d4a3fa0da11a8bf", "1b2e77c891214f9fa0add09940d13721", "9773c08cc6e64a1aaf1dc32f483e2c98", "01c6002d2d234905aea78bb02b1ccf22", "9bcf8afeefcc4296bc8e9696144f6088", "b8ca5377cfb941e19ddfb36b1debfdf9", "c6d1213f895d4cdea15730381d56dd73", "4b27b87c09d942999529accb5be11596", "bdb53b69a58c4b38becc25a8a3938fef", "437ccf680aec43cc833dd66a9e8f23ae", "c85c451a6f454d70b15fd42952588acf", "51d755f610384e3fa8231a2ec9427700", "39b9c0f5ba3647d0b8855ead4ea6b853", "b7834b3780ab449fabea0d83bb675c1d", "d2a3d926a07f4d42af09ba29584deb57", "db70779f795b4026a6cd7408f8bef8ce", "3759b8ddf4bb4f168f72b19b6c987ba4", "aa0995c36d1f4a6bbd5cd2d68c80709e", "9b33d1a8e4c24715ae50d6cd0497fbed", "1a84c397d8a348eaae684b1c9ed528a2", "497658f5acd94c028fd0329e8f60fabf", "ded65a0403bb4e0f951bef8caa72a005", "71c9bde6dfd94cfb9012082704ef028f", "2bac59e86a8042a2822de7d772c89c57", "09ade4ae292649db84ea44146c01f398", "bbe84ee0017c4628a9b76063765179be", "d6fc96995cfb49c38cf19febd1337a87", "1f3deb65a68e4fa78a17e79decc38ea4", "c6e539d2d79149e3ab84c67168c48143", "6fd6ad88f2b94f73aa03deddd97f9812", "3bce54d81d474cfdbc4898d2bad5105d", "af86494db9cb40c7a496c5529691bea8", "3947c45f24864b2aab58b3dbcdc078b6", "438cad05ce2a4ce4928a113c628b65e3", "89d210c559944beb965c24316b375250"]

    rucio = Rucio(acct=RUCIO_ACCT, hostUrl=RUCIO_URL, authUrl=RUCIO_AUTH_URL,
                  configDict={"logger": logger, "user_agent": "amaltaro/deleteRucioRules"})
    for ruleId in rules:
        resp = rucio.deleteRule(ruleId)
        logger.info("\nDeletion of rule id: {}. Was successful: {}".format(ruleId, resp))
    logger.info("Done")
