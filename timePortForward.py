from __future__ import print_function, division

from future import standard_library
standard_library.install_aliases()
from builtins import str

import logging
import sys
import timeit
from urllib.parse import urlparse, ParseResult


def portForward(port):
    """1st option: decorate class methods"""
    def portForwardDecorator(callFunc):
        urlToMangle = 'https://cmsweb'

        def portMangle(callObj, url, *args, **kwargs):

            forwarded = False
            try:
                oldUrl = urlparse(url)
                found = False
                if isinstance(url, str):
                    if url.startswith(urlToMangle):
                        netlocStr = u'%s:%d' % (oldUrl.hostname, port)
                        found = True
                elif isinstance(url, bytes):
                    if url.startswith(urlToMangle):
                        netlocStr = b'%s:%d' % (oldUrl.hostname, port)
                        found = True
                if found:
                    newUrl = ParseResult(scheme=oldUrl.scheme,
                                         netloc=netlocStr,
                                         path=oldUrl.path,
                                         params=oldUrl.params,
                                         query=oldUrl.query,
                                         fragment=oldUrl.fragment)
                    newUrl = newUrl.geturl()
                    forwarded = True
            # try:
            #     """3rd option: the simplest possible implementation"""
            #     if url.startswith("https://cmsweb"):
            #         newUrl = url.replace('.cern.ch/', '.cern.ch:8443/', 1)
            except Exception:
                pass
            if forwarded:
                return callFunc(callObj, newUrl, *args, **kwargs)
            else:
                return callFunc(callObj, url, *args, **kwargs)
        return portMangle
    return portForwardDecorator


class PortForward():
    """2nd option: wrap the decorator in a function"""
    def __init__(self, port):

        self.logger = logging.getLogger()
        self.port = port

    def __call__(self, url):
        def dummyCall(self, url):
            return url
        return portForward(self.port)(dummyCall)(self, url)


def stringReplFunc(url):
    """3rd option: the simplest possible implementation"""
    if url.startswith("https://cmsweb"):
        return url.replace('.cern.ch/', '.cern.ch:8443/', 1)


def urlParseFunc(url):
    urlToMangle = 'https:/cmsweb'
    port = 8443

    forwarded = False
    try:
        oldUrl = urlparse(url)
        found = False
        if isinstance(url, str):
            if url.startswith(urlToMangle):
                netlocStr = u'%s:%d' % (oldUrl.hostname, port)
                found = True
        elif isinstance(url, bytes):
            if url.startswith(urlToMangle):
                netlocStr = b'%s:%d' % (oldUrl.hostname, port)
                found = True
        if found:
            newUrl = ParseResult(scheme=oldUrl.scheme,
                                 netloc=netlocStr,
                                 path=oldUrl.path,
                                 params=oldUrl.params,
                                 query=oldUrl.query,
                                 fragment=oldUrl.fragment)
            newUrl = newUrl.geturl()
            forwarded = True
    except Exception:
        pass
    if forwarded:
        return newUrl
    else:
        return url


class RequestHandler(object):
    def __init__(self, config=None, logger=None):
        super(RequestHandler, self).__init__()
        if not config:
            config = {}

    @portForward(8443)
    def request(self, url, params=None, headers=None, verb='GET',
                verbose=0, ckey=None, cert=None, doseq=True,
                encode=False, decode=False, cookie=None, uri=None):
        return url


def testDecorator(payload=10):
    print("testDecorator received payload: {}".format(payload))
    payload = payload // 3  # payload is the total, so divide by 3

    reqMgr = RequestHandler()
    # url failing to be parsed
    for counter in range(int(payload)):
        reqMgr.request("bad_url", None, [], {})
    # url not matching anything
    for counter in range(int(payload)):
        reqMgr.request("https://cmsrucio.cern.ch/blah", None, [], {})
    # url matching cmsweb
    for counter in range(int(payload)):
        reqMgr.request("https://cmsweb.cern.ch/reqmgr2/data/info", None, [], {})


def testFuncDecorator(payload=10):
    print("testFuncDecorator received payload: {}".format(payload))
    payload = payload // 3  # payload is the total, so divide by 3

    portForwarder = PortForward(8443)
    # url failing to be parsed
    for counter in range(int(payload)):
        portForwarder("bad_url")
    # url not matching anything
    for counter in range(int(payload)):
        portForwarder("https://cmsrucio.cern.ch/blah")
    # url matching cmsweb
    for counter in range(int(payload)):
        portForwarder("https://cmsweb.cern.ch/reqmgr2/data/info")


def testSimpleFuncStrReplace(payload=10):
    print("testSimpleFuncStrReplace received payload: {}".format(payload))
    payload = payload // 3  # payload is the total, so divide by 3

    # url failing to be parsed
    for counter in range(int(payload)):
        stringReplFunc("bad_url")
    # url not matching anything
    for counter in range(int(payload)):
        stringReplFunc("https://cmsrucio.cern.ch/blah")
    # url matching cmsweb
    for counter in range(int(payload)):
        stringReplFunc("https://cmsweb.cern.ch/reqmgr2/data/info")


def testSimpleFuncUrlParse(payload=10):
    print("testSimpleFuncUrlParse received payload: {}".format(payload))
    payload = payload // 3  # payload is the total, so divide by 3

    # url failing to be parsed
    for counter in range(int(payload)):
        urlParseFunc("bad_url")
    # url not matching anything
    for counter in range(int(payload)):
        urlParseFunc("https://cmsrucio.cern.ch/blah")
    # url matching cmsweb
    for counter in range(int(payload)):
        urlParseFunc("https://cmsweb.cern.ch/reqmgr2/data/info")


def main():
    numTimes = sys.argv[1]
    print("Going to make {} url conversions".format(numTimes))

    # print "Started evaluation of single function call at: %s" % (datetime.utcnow())
    ti = timeit.timeit("testDecorator(%s)" % numTimes, setup="from __main__ import testDecorator", number=1)
    print("Finished single function call in {} secs\n".format(ti))

    # print "Started evaluation of %d function calls at: %s" % (numTimes, datetime.utcnow())
    ti = timeit.timeit("testFuncDecorator(%s)" % numTimes, setup="from __main__ import testFuncDecorator", number=1)
    print("Finished single function call in {} secs\n".format(ti))

    # print "Started evaluation of %d function calls at: %s" % (numTimes, datetime.utcnow())
    ti = timeit.timeit("testSimpleFuncUrlParse(%s)" % numTimes, setup="from __main__ import testSimpleFuncUrlParse", number=1)
    print("Finished single function call in {} secs\n".format(ti))

    # print "Started evaluation of %d function calls at: %s" % (numTimes, datetime.utcnow())
    ti = timeit.timeit("testSimpleFuncStrReplace(%s)" % numTimes, setup="from __main__ import testSimpleFuncStrReplace", number=1)
    print("Finished single function call in {} secs\n".format(ti))


if __name__ == "__main__":
    sys.exit(main())
