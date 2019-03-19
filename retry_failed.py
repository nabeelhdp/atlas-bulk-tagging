#!/usr/bin/python

import subprocess
import sys
import os
import time
import json
import urllib2
import base64

from tag_http import send_http_request,gen_http_request
from tag_query import gen_search_json,gen_tag_json
from process_results import extract_guidinfo
from atlas_tagging import get_config_params

def retry(configs,filename,batch):
    
    tag_url = "http://{}:{}/api/atlas/v2/entity/bulk/classification".format(
                configs['atlas_host'],
                configs['atlas_port'],
                )
       
    with open(filename) as f:
         data=json.load(f)
    
    batch_number = 0    
    failed_batches ={}
    for key in data:
         counter=0
         batch_number = batch_number + 1
         guid_batch = []
         for guid in data[key]:
            guid_batch.append(guid)
            counter = counter + 1
            if counter == batch:
               guid_data = gen_tag_json( guid=guid_batch, tagname=configs['tagname'] )
               tag_request = gen_http_request( tag_url, configs, guid_data)
               tag_response = send_http_request( tag_request, configs['timeout'] )
               try:
                   print(tag_response.read())
               except AttributeError as e:
                   print("Warning. No JSON returned. Returned object : {}".format(tag_response))
                   print("URL : {}".format(tag_url))
                   failed_batches[str(batch_number)] = guid_batch
          # Reset counters
               guid_batch = []
               counter = 0

    # Dump failed guid batches into error log file
    errorfile = open("tag_fail_guids1.json","w")
    errorfile.write(json.dumps(failed_batches))
    errorfile.close()
              
 
def main():
    
    configs = get_config_params() 
    retry(configs,'tag_fail_guids.json',1)  

if __name__ == "__main__" :
    main()

