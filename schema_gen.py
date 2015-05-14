#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import json, sys, copy, os, re
import locale, datetime
import csv
locale.setlocale(locale.LC_ALL, 'en_US')
from collections import defaultdict
from operator import itemgetter
#import pprint
#pp = pprint.PrettyPrinter(indent=4)

sys.path.append('/opt/local/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages')

def to_stderr(*objs):
    print(*objs, file=sys.stderr)

def print_time(label,time_start):
    to_stderr( label, datetime.datetime.now()-time_start )

NoneType=type(None)

"""
Builds a nested dict representation of the key paths and data types of the JSON.
Takes one object at a time, layering the schema data on top of prior iterations.
"""
def get_key_tree(json_obj,path_tree,depth=1):
    global first, again
    keys=json_obj.keys()
    for key in keys:
        vtype=type(json_obj[key])
        if vtype==NoneType:
            continue
        elif vtype==dict:
            if key not in path_tree:
                first+=1
                path_tree[key]=dict()
            get_key_tree(json_obj[key],path_tree[key],depth+1)
        elif vtype==list:
            print("list %s" % key)
            if key not in path_tree:
                first+=1
                path_tree[key]=dict()
            get_key_tree(json_obj[key],path_tree[key],depth+1)
        else:
            if key not in path_tree:
                path_tree[key]=dict()
            value_length=json_obj[key] if vtype in (int,float,bool) else len(str(json_obj[key].encode('ascii','replace')))
            if vtype.__name__ not in path_tree[key]:
                path_tree[key][vtype.__name__]=value_length
                first+=1
            else:
                path_tree[key][vtype.__name__]=max(value_length,path_tree[key][vtype.__name__])
                again+=1

def prep_predicates(filters):
    preds = []
    for filter in filters:
        (path,value) = re.split('\s*=\s*',filter)
        path_els = path.split('/')
        values = value.split(',')
        preds.append({"path_els":path_els,"values":values})
    return preds

def project_predicate_test(proj,predicates):
    v = proj
    match = False
    for pred in predicates:
        for path_el in pred['path_els']:
            v = v[path_el]
        sv=str(v)
        if (sv==pred['values']) or (type(pred['values'])==list and sv in pred['values']):
            match = True
            break
    return match

def main(wr_kickstarter_json_path,filter_predicates=[]):
    global first, again
    first,again=0,0
    predicates = prep_predicates(filter_predicates) if filter_predicates else []
    schema = gen_schema(wr_kickstarter_json_path,predicates)
    print( json.dumps(schema,indent=4,sort_keys=True) )
    print("first: %d, again: %d" % (first,again) )

def gen_schema(wr_kickstarter_json_path,predicates):
    time_import_start=datetime.datetime.now()
    with open(wr_kickstarter_json_path) as f:
        json_data = f.read()
    j = json.loads(json_data)
    print_time( "JSON Load", time_import_start )
    schema_tree = defaultdict(dict)

    time_iterate_start=datetime.datetime.now()
    for block_of_projects in j:
        proj_count = len(block_of_projects["projects"])
        for i in range(proj_count):
            proj = block_of_projects["projects"][i]
            if predicates and not project_predicate_test(proj,predicates):
                continue
            get_key_tree(proj,schema_tree)
    print_time( "Schema iterate", time_iterate_start )
    return schema_tree

if __name__ == '__main__':
    min_args = 2
    if (len(sys.argv)<min_args) or (not os.path.exists(sys.argv[1])):
        print( "Usage: %s <webrobots_ks_data.json> <usd_fx_csv>" % sys.argv[0] )
        print( "e.g. %s sample-data/five_projects_from-2014-12-02.json sample-data/usd_all_2015-03-25.csv" % sys.argv[0] )
        exit()
    main(sys.argv[1],sys.argv[2:] if len(sys.argv)>min_args else None)
