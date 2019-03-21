#!/usr/bin/python

import subprocess
import sys
import os
import time
import json
import urllib2
import base64

def gen_search_json( column ,offset,limit):

    criterion = {}
    #criterion['attributeName'] = 'qualifiedName'
    criterion['attributeName'] = 'name'
    criterion['operator'] = 'eq'
    #attributeValue = '.'.join([schema, table, '@'.join([column,cluster])])
    attributeValue = column
    criterion['attributeValue'] = attributeValue

    entityFilters = {}
    entityFilters['condition'] = 'AND'
    entityFilters['criterion'] = [criterion]

    querydata = {}
    querydata['excludeDeletedEntities'] = True
    querydata['entityFilters'] = entityFilters
    querydata['limit'] = limit
    querydata['offset'] = offset
    querydata['typeName'] = 'hive_column'

    return querydata


def gen_tag_json( tagname, guid ):

    guid_data = {}
    guid_data['classification'] = {}
    guid_data['classification']['typeName'] = tagname
    guid_data['classification']['attributes'] = {}
    guid_data['entityGuids'] = list(guid)

    return guid_data
