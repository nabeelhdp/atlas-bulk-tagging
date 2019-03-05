
#!/usr/bin/python

import subprocess
import sys
import os
import time
import json
import urllib2
import base64


def get_config_params():

    configs = {}
    configs['cluster'] = 'SCBHaaSTEST'
    configs['atlas_host'] = '10.20.174.137'
    configs['atlas_port'] = 21000
    configs['timeout'] = 30
    configs['tagname'] = 'ray'
    configs['atlas_user'] = 'admin'
    configs['atlas_pass'] = 'admin'

    return configs

def gen_search_json( cluster, schema, table, column ):

    criterion = {}
    criterion['attributeName'] = 'qualifiedName'
    criterion['operator'] = 'eq'
    attributeValue = '.'.join([schema, table, '@'.join([column,cluster])])
    criterion['attributeValue'] = attributeValue

    entityFilters = {}
    entityFilters['condition'] = 'AND'
    entityFilters['criterion'] = [criterion]

    querydata = {}
    querydata['excludeDeletedEntities'] = True
    querydata['entityFilters'] = entityFilters
    querydata['query'] = column
    querydata['offset'] = 0
    querydata['typeName'] = 'hive_column'

    return querydata

def extract_guidinfo( atlas_response, offset ):

    guid_dict = {}
    try:
        if len(atlas_response['entities']) > 0 :
            for entity in atlas_response['entities']:
                guid_dict[entity["attributes"]['qualifiedName']] = entity['guid']
                #print("Mapped keys -  {}: {}".format(entity["attributes"]['qualifiedName'],guid_dict[entity["attributes"]['qualifiedName']] ))
            if len(atlas_response['entities']) == 10000:
               offset = int(offset) + 1
            else:
               offset = 0
            return guid_dict, offset
        else:
            return 'Ignore', -1
    except KeyError as e:
        return 'Ignore', -1
    except TypeError as v:
        print (atlas_response )

def gen_tag_json( tagname, guid ):

    guid_data = {}
    guid_data['classification'] = {}
    guid_data['classification']['typeName'] = tagname
    guid_data['classification']['attributes'] = {}
    guid_data['entityGuids'] = list(guid)

    return guid_data

def gen_http_req( url, configs, data=""):

    req = ""
    auth_string = "%s:%s" % (configs['atlas_user'], configs['atlas_pass'])
    auth_encoded = 'Basic %s' % base64.b64encode(auth_string).strip()
    if data == "":
        req = urllib2.Request(url)
    else:
        req = urllib2.Request(url,data=json.dumps(data))
        print(json.dumps(data))
    req.add_header('Authorization', auth_encoded)
    req.add_header('Content-Type', 'application/json')
    req.add_header('charset', 'UTF-8')
    req.add_header('Accept', 'application/json')

    return req

def search_col_guid(colnameline):

    col_guid_map = {}
    configs = get_config_params()
    search_url_suffix = "api/atlas/v2/search/basic"
    limit = 10000
    offset = 0
    querycol = colnameline.split(",")[-1]

    while True:
        col_guid_map_partial = {}
        search_url = "http://{}:{}/{}?typeName={}&offset={}&limit={}&excludeDeletedEntities=true&query={}".format(
                configs['atlas_host'],
                configs['atlas_port'],
                search_url_suffix,
                "hive_column",
                offset,
                limit,
                querycol
                )
        #print(search_url)
        search_request = gen_http_req( search_url, configs)
        atlas_response = send_atlas_request( search_request, configs['timeout'] )
        col_guid_map_partial, offset = extract_guidinfo( atlas_response, offset )

        # If data has been added to guid map, append main dict
        if offset >= 0:
            col_guid_map = merge_dicts( col_guid_map_partial, col_guid_map)
        # If data extraction completed or no data extracted, exit loop
        if offset < 1 :
            break
        if offset > 1 :
            sr = open("skippedresults","a")
            sr.write(querycol)
            sr.close()
            break

    return col_guid_map



def merge_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z

def send_atlas_request( req, timeout):

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

def send_classify_post_request(piicolmap,configs):

    tag_url = "http://{}:{}/api/atlas/v2/entity/bulk/classification".format(
                configs['atlas_host'],
                configs['atlas_port'],
                )

    counter = 0
    guid_batch = []
    for cols in piicolmap:
       guid_batch.append(piicolmap[cols])
       counter = counter + 1
       if counter == 100:
          guid_data = gen_tag_json( guid=guid_batch, tagname=configs['tagname'] )
          tag_request = gen_http_req( tag_url, configs, guid_data)
          tag_response = send_atlas_request( tag_request, configs['timeout'] )
          try:
              print(tag_response.read())
          except AttributeError as e:
              print("Warning. No JSON returned. Returned object : {}".format(tag_response))

          # Reset counters
          guid_batch = []
          counter = 0

def main():

    input_file = sys.argv[1]
    colentries = []
    with open(input_file) as f:
        colentries_ = f.readlines()

    colentries = [s.replace("\r\n", "") for s in colentries_]

    # lastcols = [c.split(",")[-1] for c in clines]
    # uniqlastcols = list(set(lastcols))

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
        else:
            pass

    print("Total number of columns with guid mapped = {}".format(len(col_guid_map)))
    #sample = [ col_guid_map[k] for k in col_guid_map.keys()[:2000]]
    #print( '","'.join(str(x) for x in sample))

    piicolmap = {}

    for querycol in colentries:
        x = querycol.split(",")
        db = x[0]
        tbl = x[1]
        col = x[2]
        colfull = db + "." + tbl + "." + col + "@" + configs['cluster']
        try:
            piicolmap[querycol] = col_guid_map[colfull]
        except KeyError as k:
            print ("Column not found in Atlas store: {}".format(querycol))

    piijson = json.dumps(piicolmap)
    piifile = open("pii_guid_map.json","w")
    piifile.write(piijson)
    piifile.close()

    # send_classify_post_request(piicolmap=piicolmap,configs=configs)

if __name__ == "__main__":
    main()
