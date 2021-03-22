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
        urlMangleList = ['https://tivanov',
                         'https://alancc',
                         'https://cmsweb']

        def portMangle(callObj, url, *args, **kwargs):
            # As a first step try to get a logger from the calling object:
            if callable(getattr(callObj, 'logger', None)):
                logger = callObj.logger
            else:
                logger = logging.getLogger()

            forwarded = False
            try:
                oldUrl = urlparse(url)
                found = False
                if isinstance(url, str):
                    for mUrl in urlMangleList:
                        if url.startswith(mUrl):
                            netlocStr = u'%s:%d' % (oldUrl.hostname, port)
                            found = True
                            break
                elif isinstance(url, bytes):
                    for mUrl in urlMangleList:
                        if url.startswith(mUrl.encode('utf-8')):
                            netlocStr = b'%s:%d' % (oldUrl.hostname, port)
                            found = True
                            break
                if found:
                    newUrl = ParseResult(scheme=oldUrl.scheme,
                                         netloc=netlocStr,
                                         path=oldUrl.path,
                                         params=oldUrl.params,
                                         query=oldUrl.query,
                                         fragment=oldUrl.fragment)
                    newUrl = newUrl.geturl()
                    forwarded = True
            except Exception as ex:
                msg = "Failed to forward url: %s to port: %s due to ERROR: %s"
                logger.exception(msg, url, port, str(ex))
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


def replacePort(url):
    """3rd option: the simplest possible implementation"""
    if url.startswith("https://cmsweb"):
        return url.replace('.cern.ch/', '.cern.ch:8443/', 1)


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


def testSimpleFunc(payload=10):
    print("testSimpleFunc received payload: {}".format(payload))
    payload = payload // 3  # payload is the total, so divide by 3

    # url failing to be parsed
    for counter in range(int(payload)):
        replacePort("bad_url")
    # url not matching anything
    for counter in range(int(payload)):
        replacePort("https://cmsrucio.cern.ch/blah")
    # url matching cmsweb
    for counter in range(int(payload)):
        replacePort("https://cmsweb.cern.ch/reqmgr2/data/info")


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
    ti = timeit.timeit("testSimpleFunc(%s)" % numTimes, setup="from __main__ import testSimpleFunc", number=1)
    print("Finished single function call in {} secs\n".format(ti))


if __name__ == "__main__":
    sys.exit(main())
