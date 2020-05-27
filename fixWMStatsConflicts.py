"""
Fix conflicts in central WMStats database
"""
from __future__ import print_function
import sys
from pprint import pformat
from WMCore.Database.CMSCouch import Database

URL = 'https://cmsweb.cern.ch/couchdb'
centralWMStats = Database('wmstats', 'https://cmsweb.cern.ch/couchdb')


def resolveConflict(docInfo):
    "It only works for 2 revision conflicts"
    print("Resolving conflicts for doc id: {} with {} revisions".format(docInfo["id"], len(docInfo["value"])))
    doc1 = centralWMStats.document(docInfo["id"], docInfo["value"][0])
    doc2 = centralWMStats.document(docInfo["id"], docInfo["value"][1])
    if doc1.get("timestamp") == doc2.get("timestamp"):
        # can't figure out which doc needs to be kept
        print("  Cannot resolve conflict because docs don't have a timestamp key")
        print("  Doc1: {}".format(pformat(doc1)))
        print("  Doc2: {}".format(pformat(doc2)))
    elif doc1.get("timestamp") > doc2.get("timestamp"):
        # then delete the younger document, thus doc2
        print("  Deleting doc2 id: {}, with rev: {}".format(docInfo["id"], docInfo["value"][1]))
        centralWMStats.delete_doc(docInfo["id"], docInfo["value"][1])
    elif doc1.get("timestamp") < doc2.get("timestamp"):
        # then doc1 is the younger one, delete it
        print("  Deleting doc2 id: {}, with rev: {}".format(docInfo["id"], docInfo["value"][1]))
        centralWMStats.delete_doc(docInfo["id"], docInfo["value"][1])


def main():
    wmstatsDB = Database('wmstats', 'https://alancc7-cloud3.cern.ch/couchdb')
    conflictDocs = wmstatsDB.loadView("WMStats3", "conflicts")
    print("Found {} conflicting documents".format(conflictDocs.get("total_rows")))
    print("    they are:\n{}".format(pformat(conflictDocs.get("rows", []))))
    for doc in conflictDocs.get("rows", []):
        resolveConflict(doc)

if __name__ == "__main__":
    sys.exit(main())
