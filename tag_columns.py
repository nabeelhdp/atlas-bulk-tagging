#!/usr/bin/python

import json
from atlas_tagging import get_config_params,send_classify_post_request

guids = {}
with open('pii_guid_map.json') as f:
    guids = json.load(f)

configs = get_config_params()
configs['tagname'] = 'March2'
send_classify_post_request(guids,configs) 
