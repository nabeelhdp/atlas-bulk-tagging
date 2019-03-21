#!/usr/bin/python

import subprocess
import sys
import os
import time
import json
import urllib2
import base64

def gen_http_request( url, configs, data=""):

    req = ""
    auth_string = "%s:%s" % (configs['atlas_user'], configs['atlas_pass'])
    auth_encoded = 'Basic %s' % base64.b64encode(auth_string).strip()
    if data == "":
        req = urllib2.Request(url)
    else:
        req = urllib2.Request(url,data=json.dumps(data))
        #print(json.dumps(data))
    req.add_header('Authorization', auth_encoded)
    req.add_header('Content-Type', 'application/json')
    req.add_header('charset', 'UTF-8')
    req.add_header('Accept', 'application/json')

    return req

def send_http_request( req, timeout):

    httpHandler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(httpHandler)
    end = 0
    start = 0
    try:
       start = time.time()
       response = opener.open(req)
       end = time.time()
       print "Request to {} took {} seconds".format( req.get_full_url(), end - start)
       return json.load(response)
    except (urllib2.URLError, urllib2.HTTPError) as e:
       print 'Error', e
    except ValueError as e:
       print("Empty response likely {}".format(response))
       return response
