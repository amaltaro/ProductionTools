#!/usr/bin/env python
"""
Script providing basic interaction to the Rucio REST APIs
"""
from __future__ import print_function

import httplib
import json
import os
import sys
from pprint import pformat

url = 'cms-rucio-int.cern.ch'
urlAuth = "cms-rucio-auth-int.cern.ch"


def ping():
    """
    Just a Server ping test - no credentials required
    """
    conn = httplib.HTTPConnection(url)
    urn = "/ping"
    conn.request("GET", urn)
    res = conn.getresponse()
    if res.status != 200:
        print("Failed to ping the Rucio server. Response status: %s and reason: %s" % (res.status, res.reason))
        return
    data = res.read()
    print("PING response: %s" % data)
    return data


def authenticate():
    """
    Provided a Rucio account, fetch a token from the authentication server
    """
    headers = {"X-Rucio-Account": "wma_test"}

    conn = httplib.HTTPSConnection(urlAuth, cert_file=os.getenv('X509_USER_CERT'), key_file=os.getenv('X509_USER_KEY'))
    urn = "/auth/x509"
    conn.request("GET", urn, headers=headers)
    res = conn.getresponse()
    if res.status != 200:
        print("Failed to get a token from Rucio. Response status: %s and reason: %s" % (res.status, res.reason))
        return
    token = res.getheader('x-rucio-auth-token', None)
    validity = res.getheader('x-rucio-auth-token-expires', None)
    print("AUTH retrieved a token: %s\twith validity: %s" % (token, validity))
    return token


def validate(token):
    """
    Provided a Rucio token, check it's lifetime and extend it by another hour
    """
    if not token:
        print("Received an invalid token: %s" % token)
        return

    headers = {"X-Rucio-Auth-Token": token}

    conn = httplib.HTTPSConnection(urlAuth)
    urn = "/auth/validate"
    conn.request("GET", urn, headers=headers)
    res = conn.getresponse()
    if res.status != 200:
        print("Failed to renew the Rucio token. Response status: %s and reason: %s" % (res.status, res.reason))
        return

    print("VALIDATE response: %s" % res.read())
    return


def getDID(token, dataId):
    """
    Provided a Rucio token and a data identifier, retrieve basic information
    about the data object
    """
    if not token:
        print("Received an invalid token: %s" % token)
        return

    headers = {"X-Rucio-Auth-Token": token,
               "Content-type": "application/json",
               "Accept": "application/json"}

    conn = httplib.HTTPSConnection(url)
    urn = '/dids/cms/%s' % dataId
    conn.request("GET", urn, headers=headers)
    res = conn.getresponse()
    if res.status != 200:
        print("Failed to get DID from Rucio. Response status: %s and reason: %s" % (res.status, res.reason))
        print("Output: %s" % res.read())
        return
    data = json.loads(res.read())
    print("DID data retrieved:\n%s" % pformat(data))
    return data


def main():
    if len(sys.argv) != 2:
        print("usage: python getRucioDID.py <data identifier name>")
        print("  Ex.: python getRucioDID.py /QCD_Pt_120to170_TuneCUETP8M1_13TeV_pythia8/RunIISummer16NanoAODv6-PUMoriond17_Nano25Oct2019_102X_mcRun2_asymptotic_v7_ext1-v1/NANOAODSIM")
        sys.exit(0)
    did = sys.argv[1]
    ping()
    token = authenticate()
    validate(token)
    getDID(token, did)

    print("Done!")


if __name__ == "__main__":
    main()
