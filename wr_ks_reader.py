#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json, sys, copy, os
import pprint
import locale
import csv
locale.setlocale(locale.LC_ALL, 'en_US')
from collections import defaultdict
pp = pprint.PrettyPrinter(indent=4)


"""
Builds a nested dict representation of the key paths and data types of the JSON.
Takes one object at a time, layering the schema data on top of prior iterations.
"""
def get_key_tree(json_obj,path_tree,depth=1):
    keys = json_obj.keys()
    for key in keys:
        ktype = type(json_obj[key])
        if ktype == dict:
            path_tree[key] = dict()
            get_key_tree(json_obj[key],path_tree[key],depth+1)
        elif ktype == list:
            path_tree[key] = list()
            get_key_tree(json_obj[key],path_tree[key],depth+1)
        else:
            if key not in path_tree:
                path_tree[key] = dict()
            if ktype.__name__ not in path_tree[key]:
                path_tree[key][ktype.__name__] = 1
            else:
                path_tree[key][ktype.__name__] += 1

""" 
Iterate a dictionary, generate a string buffer of key\tvalue pairs, assuming number
Allows second dictionary (d2), treated as denominator 
"""
def dict_value_sort(d,d2=None):
    hdr="\n\tUSD"
    if d2!=None:
        hdr+="\t#Projects\tUSD/Proj\n"
    buf=""
    for key in sorted(d, key=d.get, reverse=True):
        buf += "%s\t%s" % (key, locale.format("%12d", d[key], grouping=True))
        if d2 != None:
            buf += "\t%s\t%s" % (locale.format("%6d", d2[key], grouping=True), locale.format("%6d", d[key]/d2[key], grouping=True))
        buf += "\n"
    return hdr+buf

def read_usd_fx_table(usd_fx_csv_pathname):
    fxusd = dict();
    with open(usd_fx_csv_pathname, 'rb') as csvfile:
        fxreader = csv.reader(csvfile, delimiter=',')
        for row in fxreader:
            if len(row)>0 and row[0]!='Currency':
                fxusd[row[1]] = {
                    'Name': row[0],
                    'cur_buys_usd': float(row[2]),
                    'usd_buys_cur': float(row[3])
                }
    return fxusd

def main(wr_kickstarter_json_path,usd_fx_pathname):
    fxusd = read_usd_fx_table(usd_fx_pathname)
    (report,schema) = gen_ks_report(wr_kickstarter_json_path,fxusd)
    print json.dumps(schema,indent=4)
    print report

def gen_ks_report(wr_kickstarter_json_path,fxusd):
    json_data = open(wr_kickstarter_json_path).read()
    j = json.loads(json_data)
    schema_tree = defaultdict(dict)
    tots = defaultdict(dict)

    template = {
        "pled_ctry"  : dict(),
        "goal_ctry"  : dict(),
        "cnt_ctry"  : dict(),
        "pled_cat"   : dict(),
        "goal_cat"   : dict(),
        "cnt_cat"   : dict(),
        "pled_state" : 0,
        "goal_state" : 0,
        "cnt_state" : 0
    }

    """
    tots = {
        "live" : copy.deepcopy(template),
        "successful" : copy.deepcopy(template),
        "failed" : copy.deepcopy(template),
        "canceled" : copy.deepcopy(template),
        "suspended" : copy.deepcopy(template)
    }
    """
    """
92562 failed
74635 successful
17296 canceled
 6496 live
  395 suspended
    """

    cnt_all = 0
    for block_of_projects in j:
        projects = block_of_projects["projects"]
        proj_count = len(projects)
        for i in range(proj_count):
            proj = projects[i]
            # Build schema
            get_key_tree(proj,schema_tree)
            # Grab project values
            pled = proj["pledged"] * fxusd[proj["currency"]]["cur_buys_usd"]
            #goal = proj["goal"] * fxusd[proj["currency"]]["cur_buys_usd"]
            ctry = proj["country"]
            cat = proj["category"]["name"]
            state = proj["state"]

            # Ensure accumulation skeleton exists
            if state not in tots:
                tots[state] = copy.deepcopy(template)
            # Accumulate totals, increment counters
            tots[state]["pled_ctry"][ctry] = tots[state]["pled_ctry"][ctry] + pled if ctry in tots[state]["pled_ctry"] else pled
            #tots[state]["goal_ctry"][ctry] = tots[state]["goal_ctry"][ctry] + goal if ctry in tots[state]["goal_ctry"] else goal
            tots[state]["cnt_ctry"][ctry] = tots[state]["cnt_ctry"][ctry] + 1 if ctry in tots[state]["cnt_ctry"] else 1

            tots[state]["pled_cat"][cat] = tots[state]["pled_cat"][cat] + pled if cat in tots[state]["pled_cat"] else pled
            #tots[state]["goal_cat"][cat] = tots[state]["goal_cat"][cat] + goal if cat in tots[state]["goal_cat"] else goal
            tots[state]["cnt_cat"][cat] = tots[state]["cnt_cat"][cat] + 1 if cat in tots[state]["cnt_cat"] else 1

            tots[state]["pled_state"] += pled
            #tots[state]["goal_state"] += goal
            tots[state]["cnt_state"] += 1
            cnt_all += 1

    # Generate the report
    buf = ""
    for state in tots:
        buf += "Per country, %s: %s\n" % (state,dict_value_sort(tots[state]["pled_ctry"],tots[state]["cnt_ctry"]))
        buf += "Per category, %s: %s\n" % (state,dict_value_sort(tots[state]["pled_cat"],tots[state]["cnt_cat"]))
        buf += "Pledged overall for %s: %s\n" % (state,locale.format("%6d", tots[state]["pled_state"], grouping=True))
        #buf += "Goal overall for %s: %s\n" % (state,locale.format("%6d", tots[state]["goal_state"], grouping=True))
        buf += "Count overall for %s: %s\n" % (state,locale.format("%6d", tots[state]["cnt_state"], grouping=True))
        buf += "Per project for %s: %s\n" % (state,locale.format("%6d", tots[state]["pled_state"]/tots[state]["cnt_state"], grouping=True))
        buf += "'%s\n" % ("=" * 40)
    buf += "Number of projects, overall: %d\n" % cnt_all
    return (buf,schema_tree)

if __name__ == '__main__':
    if (len(sys.argv)<3) or not os.path.exists(sys.argv[1]) or not os.path.exists(sys.argv[2]):
        print "Usage: wr_ks_reader.py <werobots_ks_data.json> <usd_fx_csv>"
        print "e.g. ./wr_ks_reader.py sample-data/five_projects_from-2014-12-02.json sample-data/usd_all_2015-03-25.csv"
        exit()
    main(sys.argv[1],sys.argv[2])
