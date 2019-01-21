import sys
import time
import timeit
import uuid


def testFun(payload=[]):
    ts = int(time.time())
    if isinstance(payload, dict):
        payload = [payload]  # it was a single document
    commonHeaders = {'type': "doc_type",
                     'version': 0.2,
                     'producer': "producer_blah"}

    docs = []
    for doc in payload:
        notification = {}
        notification.update(commonHeaders)
        # Add body consisting of the payload and metadata
        body = {'payload': doc,
                'metadata': {'timestamp': ts,
                             'id': "docId_Blah",
                             'uuid': str(uuid.uuid4())}
                }
        notification['body'] = body
        docs.append(notification)

        return docs


def testFunSingle(numTimes):
    for i in range(0, numTimes):
        testFun([{str(i): i}])


def main():
    myDict = {}
    numTimes = int(1e6)
    for i in range(0, numTimes):
        myDict.update({str(i): i})

    # print "Started evaluation of single function call at: %s" % (datetime.utcnow())
    ti = timeit.timeit("testFun([%s])" % myDict, setup="from __main__ import testFun", number=1)
    print "Finished single function call in %s secs" % ti

    # print "Started evaluation of %d function calls at: %s" % (numTimes, datetime.utcnow())
    ti = timeit.timeit("testFunSingle(%d)" % numTimes, setup="from __main__ import testFunSingle", number=1)
    print "Finished %d function calls in %s secs" % (numTimes, ti)


if __name__ == "__main__":
    sys.exit(main())
