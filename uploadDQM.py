#!/usr/bin/env python
from __future__ import print_function

import httplib
import os
import os.path
import ssl
import sys
import urllib2
from cStringIO import StringIO
from functools import reduce
from gzip import GzipFile
from hashlib import md5
from mimetypes import guess_type
from pprint import pprint


class HTTPSAuthHandler(urllib2.HTTPSHandler):
    """
    HTTPS authentication class to provide path of key/cert files.
    """

    def __init__(self, key=None, cert=None, capath='/etc/grid-security/certificates/', level=0):
        if cert:
            # then create a default ssl context manager to carry the credentials
            # loading default CA certificates
            self.ctx = ssl.create_default_context()
            self.ctx.load_cert_chain(cert, keyfile=key)
            # if not len(self.ctx.get_ca_certs()):
            #    print("No trusted CA certificates were automatically found.")
            # then let's try just loading the usual CA location
            self.ctx.load_verify_locations(None, capath)

            urllib2.HTTPSHandler.__init__(self, debuglevel=level, context=self.ctx)
            print("AMR listing ssl context properties for %s" % repr(self.ctx))
            print("check_hostname : %s" % self.ctx.check_hostname)  # default to True
            print("options : %s" % self.ctx.options)  # couldn't find what this options are
            print("protocol : %s" % self.ctx.protocol)  # default to PROTOCOL_SSLv23
            print("verify_flags : %s" % self.ctx.verify_flags)  # default to VERIFY_DEFAULT
            print("verify_mode : %s" % self.ctx.verify_mode)  # default to CERT_REQUIRED
            print("Found %d default trusted CA certificates." % len(self.ctx.get_ca_certs()))
            for ca in self.ctx.get_ca_certs():
                if 'CERN' in str(ca['subject']):
                    print("  %s" % str(ca['subject']))

        else:
            print("No ssl context provided")
            urllib2.HTTPSHandler.__init__(self, debuglevel=level)

    def get_connection(self, host, **kwargs):
        if self.ctx:
            return httplib.HTTPSConnection(host, context=self.ctx, **kwargs)
        return httplib.HTTPSConnection(host, **kwargs)

    def https_open(self, req):
        """
        Overwrite the default https_open.
        """
        print("AMR object: %s" % repr(req))
        print("AMR dir: %s" % dir(req))
        print("AMR get_method: %s" % req.get_method())
        print("AMR get_type: %s" % req.get_type())
        print("AMR get_selector(): %s" % req.get_selector())
        print("AMR has_proxy: %s" % req.has_proxy())
        print("AMR headers: %s" % req.headers)
        print("AMR get_full_url: %s" % req.get_full_url())
        print("AMR has_data: %s" % req.has_data())
        print("AMR host: %s" % req.host)
        print("AMR _tunnel_host: %s\n" % req._tunnel_host)
        # print("AMR data: %s" % req.data[:200])
        return self.do_open(self.get_connection, req)


def encode(args, files):
    boundary = '----------=_DQM_FILE_BOUNDARY_=-----------'
    (body, crlf) = ('', '\r\n')
    for (key, value) in args.items():
        payload = str(value)
        body += '--' + boundary + crlf
        body += ('Content-Disposition: form-data; name="%s"' % key) + crlf
        body += crlf + payload + crlf
    for (key, filename) in files.items():
        body += '--' + boundary + crlf
        body += ('Content-Disposition: form-data; name="%s"; filename="%s"'
                 % (key, os.path.basename(filename))) + crlf
        body += ('Content-Type: %s' % filetype(filename)) + crlf
        body += ('Content-Length: %d' % os.path.getsize(filename)) + crlf
        body += crlf + open(filename, "r").read() + crlf
    body += '--' + boundary + '--' + crlf + crlf
    return ('multipart/form-data; boundary=' + boundary, body)


def filetype(filename):
    return guess_type(filename)[0] or 'application/octet-stream'


def marshall(args, files, request):
    (contentType, body) = encode(args, files)
    request.add_header('Content-Type', contentType)
    request.add_header('Content-Length', str(len(body)))
    request.add_data(body)
    return


def upload(url, args, filename):
    ### DEBUG START ###
    print("AMR X509_USER_PROXY: %s" % os.environ.get('X509_USER_PROXY'))
    print("AMR sys.prefix: %s" % sys.prefix)
    print("AMR sys.version: %s" % sys.version)
    # for param, value in os.environ.iteritems():
    #    print("%s : %s" % (param, value))

    ### DEBUG END ###
    uploadProxy = os.environ.get('X509_USER_PROXY', None)
    ident = "WMAgent python/%d.%d.%d" % sys.version_info[:3]

    msg = "HTTP POST upload arguments:\n"
    for arg in args:
        msg += "  ==> %s: %s\n" % (arg, args[arg])
    print(msg)

    handler = HTTPSAuthHandler(key=uploadProxy, cert=uploadProxy)
    # opener = urllib2.build_opener(handler)
    opener = urllib2.OpenerDirector()
    opener.add_handler(handler)

    # setup the request object
    datareq = urllib2.Request(url + '/data/put')
    datareq.add_header('Accept-encoding', 'gzip')
    datareq.add_header('User-agent', ident)
    marshall(args, {'file': filename}, datareq)

    pprint(opener.addheaders)
    if 'https://' in url:
        print("AMR calling open")
        result = opener.open(datareq)
    else:
        result = urllib2.build_opener(urllib2.ProxyHandler({})).open(datareq)

    data = result.read()
    if result.headers.get('Content-encoding', '') == 'gzip':
        data = GzipFile(fileobj=StringIO(data)).read()

    return (result.headers, data)


if __name__ == '__main__':
    args = {}
    # FIXME: read these arguments from the command line
    filename = '/afs/cern.ch/user/a/amaltaro/workarea/harvest/DQM_V0001_R000000001__RelValH125GGgluonfusion_13__CMSSW_8_1_0-RecoFullPU_2017PU_TaskChain_PUMCRecyc_HG1705_Validation_TEST_Alan_v67-v11__DQMIO.root'
    URL = 'https://cmsweb-dev.cern.ch/dqm/dev'

    # Preparing a checksum
    blockSize = 0x10000


    def upd(m, data):
        m.update(data)
        return m


    fd = open(filename, 'rb')
    try:
        contents = iter(lambda: fd.read(blockSize), '')
        m = reduce(upd, contents, md5())
    finally:
        fd.close()

    args['checksum'] = 'md5:%s' % m.hexdigest()
    # args['checksum'] = 'md5:%s' % md5.new(filename).read()).hexdigest()
    args['size'] = os.path.getsize(filename)

    msg = "HTTP Upload is about to start:\n"
    msg += " => URL: %s\n" % URL
    msg += " => Filename: %s\n" % filename
    print(msg)

    (headers, data) = upload(URL, args, filename)
    msg = 'HTTP upload finished succesfully with response:\n'
    msg += 'Status code: %s\n' % headers.get("Dqm-Status-Code", None)
    msg += 'Message: %s\n' % headers.get("Dqm-Status-Message", None)
    msg += 'Detail: %s\n' % headers.get("Dqm-Status-Detail", None)
    msg += 'Data: %s\n' % str(data)
    print(msg)
