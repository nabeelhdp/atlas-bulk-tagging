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

def get_config_params():

    configs = {}
    configs['cluster'] = 'SCBHaaSTEST'
    configs['atlas_host'] = '10.20.174.137'
    configs['atlas_port'] = 21000
    configs['timeout'] = 30
    configs['tagname'] = 'March5'
    configs['atlas_user'] = 'admin'
    configs['atlas_pass'] = 'admin'

    return configs

def search_col_guid(colnameline):

    col_guid_map = {}
    empty_dict = {}
    configs = get_config_params()
    url = "http://{}:{}/api/atlas/v2/search/basic".format(
                    configs['atlas_host'],
                    configs['atlas_port']
                    )

    limit = 10000
    # offset variables - one each for each method used to search
    offset = 0
    skipped_results = 0
    column = colnameline.split(",")[-1]
	
    col_guid_map_merged = {}
    search_suffix = "?typeName=hive_column&offset={}&limit={}&excludeDeletedEntities=true&query={}".format(
                offset,
                limit,
                column
                )
    if column in ["name","id","address","cty_code"] :
        offset = limit
    else :

        search_url = "{}{}".format(url,search_suffix)
        search_request = gen_http_request( search_url, configs)
        atlas_response = send_http_request( search_request, configs['timeout'] )
        col_guid_map, offset = extract_guidinfo( atlas_response, offset, limit )
		
    if offset == 0:
        return col_guid_map

    # If data extraction completed or no data extracted, exit loop
    if offset == -1 :
        return empty_dict

    # Reset variables for POST method
    limit = 1000
    search_url = url
    offset_post = 0

    while True:

        if offset > 0 :

            # If results of indexed search exceed one round, move the query to POST based search.
            # gen_search_json returns a dictionary object with the parameters set correctly

            search_data = gen_search_json(column,offset=offset_post,limit=limit)        
            search_request = gen_http_request( search_url,configs,search_data)
            atlas_response = send_http_request( search_request, configs['timeout'] )
            col_guid_map, offset = extract_guidinfo( atlas_response, offset_post, limit )

            if offset == 0 :
                col_guid_map_merged = merge_dicts( col_guid_map_merged, col_guid_map)
                break
            
            if offset == -1 :
                return empty_dict
 
            if offset > 0 :
                #print(col_guid_map)
                col_guid_map_merged = merge_dicts( col_guid_map_merged, col_guid_map)
                offset_post = offset
        			
    return col_guid_map_merged


def merge_dicts(x, y):
   
    z = x.copy()
    z.update(y)
    return z

def send_classify_post_request(piicolmap,configs):
    
    tag_url = "http://{}:{}/api/atlas/v2/entity/bulk/classification".format(
                configs['atlas_host'],
                configs['atlas_port'],
                )
    
    counter = 0
    guid_batch = []
    failed_batches ={}
    batch_number = 0

    for cols in piicolmap:
       batch_number = batch_number + 1
       guid_batch.append(piicolmap[cols])
       counter = counter + 1
       if counter == 100:
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
    errorfile = open("tag_fail_guids.json","w")
    errorfile.write(json.dumps(failed_batches))
    errorfile.close() 
        

def main():

    input_file = sys.argv[1]
    colentries = []
    with open(input_file) as f:
        colentries_ = f.readlines()
    colentries = [s.replace("\n", "") for s in colentries_]
   
    tagurl_search = "query=cty_code&type=hive_column&tag=March5"

    configs = get_config_params()
    uniqcols_searched = {}
    cache_state = {}
    col_guid_map = {}

    for querycol in colentries:
        # Update cache with column to guid map if not already looked up earlier
        if cache_state.get(querycol.split(",")[-1], "MISSING" ) == "MISSING" :
            tmp_col_guid_map = search_col_guid( querycol )
            col_guid_map = merge_dicts( col_guid_map, tmp_col_guid_map )
            cache_state[querycol.split(",")[-1]] = "CACHED"

    print("Total number of columns with guid mapped = {}".format(len(col_guid_map)))

    #sample = [ col_guid_map[k] for k in col_guid_map.keys()[:2000]]
    #print( '","'.join(str(x) for x in sample))

    piicolmap = {}
    
    # Filter only PII columns to be written in the persistent map file
    for querycol in colentries:
        x = querycol.split(",")
        db = x[0]
        tbl = x[1]
        col = x[2]
        colfull = db + "." + tbl + "." + col + "@" + configs['cluster']
        try:
            piicolmap[querycol] = col_guid_map[colfull]
        except KeyError as k:
            print ("Column not found in Atlas store: {}".format(colfull))

    piijson = json.dumps(piicolmap)
    piifile = open("pii_guid_map.json","w")
    piifile.write(piijson)
    piifile.close()    

    send_classify_post_request(piicolmap=piicolmap,configs=configs)

if __name__ == "__main__":
    main()

