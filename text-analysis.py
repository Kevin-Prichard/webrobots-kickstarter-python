#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import threading, traceback
import json, sys, copy, os, re, datetime, errno, urlparse
import hashlib
import pprint
import locale
import csv
import pycurl
from BeautifulSoup import BeautifulSoup
from lxml import etree

locale.setlocale(locale.LC_ALL, 'en_US')
from collections import defaultdict
from operator import itemgetter
pp = pprint.PrettyPrinter(indent=4)

#from pyteaser import SummarizeUrl
#from pattern.vector import Document, Model, Cluster, Vector, HIERARCHICAL, COSINE
from db import models as ks_models

SHOW_TIMING=False

def to_stderr(*objs):
    if SHOW_TIMING:
        print(*objs, file=sys.stderr)

def prep_predicates(filters):
    preds = []
    for filter in filters:
        (path,value) = re.split('\s*=\s*',filter)
        path_els = path.split('/')
        values = value.split(',')
        preds.append({"path_els":path_els,"values":values})
    return preds

def project_predicate_test(proj,predicates):
    match_count = len(predicates)
    for pred in predicates:
        v = proj
        for path_el in pred['path_els']:
            v = v[path_el]
        sv=str(v)
        if (sv==pred['values']) or (type(pred['values'])==list and sv in pred['values']):
            match_count -= 1
    return match_count<=0

def main(wr_kickstarter_json_path,filter_predicates=[]):
    predicates = prep_predicates(filter_predicates) if filter_predicates else []
    fetch_page_fs_init(os.path.curdir)
    gen_ks_report(wr_kickstarter_json_path,predicates)



def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        #print(traceback.format_exc(exc)) #sys.last_traceback("TRY mkdir_p")
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def fetch_page_fs_init(base_path):
    global url_cache
    url_cache = "%s/%s" % (base_path,"url_cache")
    mkdir_p( url_cache )

def get_http(url):
    to_stderr("curling")
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.FOLLOWLOCATION, True)
    c.setopt(pycurl.CONNECTTIMEOUT, 8)
    c.setopt(pycurl.TIMEOUT, 8)
    c.setopt(pycurl.NOSIGNAL, 1)
    c.setopt(pycurl.HTTPGET, 1)
    try:
        from io import BytesIO
    except ImportError:
        from StringIO import StringIO as BytesIO
    buffer=BytesIO()
    c.setopt(c.WRITEDATA, buffer)
    c.perform()
    buffer.seek(0)
    content = buffer.read()
    return content

def get_cache_fs_pathname(url):
    global url_cache
    m = hashlib.md5()
    m.update(url)
    url_hash = m.hexdigest()
    url_cache_hash_path = os.path.join( url_cache, url_hash )
    return url_cache_hash_path

def get_cache_fs(url):
    cached = False
    url_cache_hash_path = get_cache_fs_pathname(url)
    if os.path.isfile(url_cache_hash_path):
        cached = True
        with open(url_cache_hash_path,"r") as f:
            content = f.read()
    else:
        content=None
    return content,cached

def put_cache_fs(url,content):
    url_cache_hash_path = get_cache_fs_pathname(url)
    with open(url_cache_hash_path,"w") as f:
        f.write(content)

def get_cache_db(wanted_url):
    content=None
    try:
        content = ks_models.HttpCache.get(ks_models.HttpCache.url==wanted_url)
        cached = True
    except:
        cached = False
    return content.content,cached

def put_cache_db(have_url,content):
    ks_models.HttpCache._create(new_url=have_url,blob=content)

def fetch_page_fs(url):
    content,cached = get_cache_fs(url)
    if not cached:
        content = get_http(url)
        put_cache_fs(url,content)
    return content,cached

def fetch_page_db(url):
    content,cached = get_cache_db(url)
    #print("HAS CONTENT ALREADY (%d)" % len(content) if cached else "NO CONTENT")
    if not cached:
        content = get_http(url)
        put_cache_db(url,content)
    return content,cached

def fetch_page(url):
    content,cached = fetch_page_db(url)
    return content,cached

def handle_project(proj):
    global corpus, cnt
    sploded = urlparse.urlparse(proj["urls"]["web"]["project"])
    base_url = "%s://%s/%s" % (sploded.scheme,sploded.netloc,sploded.path)
    proj_desc_url = '%s/description' % base_url
    time_start = datetime.datetime.now()
    proj_desc_html,in_cache = fetch_page(proj_desc_url)
    print_time( "Page fetch %s" % base_url, time_start )
    #to_stderr( "URL(%s) = %s" % ("cached" if in_cache else "NEW", proj_desc_url) )

    time_start = datetime.datetime.now()
    proj_desc_soup = BeautifulSoup(proj_desc_html)
    print_time( "BS %s" % base_url, time_start )

    time_start = datetime.datetime.now()
    parser = etree.HTMLParser()
    tree = etree.fromstring(unicode(proj_desc_soup), parser)
    print_time( "parser+tree %s" % base_url, time_start )

    #proj_desc_paras = tree.xpath('//*[@id="content-wrap"]/div[4]/section[1]/div/div/div/div/div[1]//p')
    #proj_desc_paras = tree.xpath('//text()')
    time_start = datetime.datetime.now()
    proj_desc_paras = tree.xpath('//div/text() | //p/text()')
    text_content = " ".join("%s\n" % t for t in proj_desc_paras if t!='\n')
    print_time( "xpath+join %s" % base_url, time_start )
    #print text_content

    #proj_subset[0]["projects"].append(proj)
    # More text in body of url/description: $x('//*[@id="content-wrap"]/div[4]/section[1]/div/div/div/div/div[1]//p')
    #proj["name"].lower()
    body = "%s %s" % (proj["blurb"].lower(), text_content)
    corpus.append( { "url":base_url, "content":"%s %s" % (proj["name"].lower(), body) } )
    cnt+=1
    return in_cache


def print_time(label,time_start):
    to_stderr( label, datetime.datetime.now()-time_start )

def gen_ks_report(wr_kickstarter_json_path,predicates=[]):
    global cnt, corpus
    time_import_start=datetime.datetime.now()
    json_data = open(wr_kickstarter_json_path).read()
    j = json.loads(json_data)
    print_time( "JSON Load", time_import_start )
#    to_stderr( time_import_stop-time_import_start )
    corpus = []
    #proj_subset = [{"projects":[]}]

    time_iterate_start=datetime.datetime.now()
    cnt=0
    t=""
    done=False
    cnt2=0
    for block_of_projects in j:
        proj_count = len(block_of_projects["projects"])
        for i in range(proj_count):
            proj = block_of_projects["projects"][i]
            r_proj = ks_models.Project._create(proj)
            if predicates and not project_predicate_test(proj,predicates):
                continue
            try:
                handle_project(proj)
                #done=not handle_project(proj)
            except Exception as e:
                print(traceback.format_exc(e))
                done=True
            if done: break
            cnt2+=1
            #done=cnt2>10
        if done: break

    time_iterate_stop=datetime.datetime.now()
    #print time_iterate_stop-time_iterate_start
    #print "Count: %d" % cnt

    from textblob import TextBlob
    noun_phrase_index={}
    with open("/tmp/stuff.txt","w") as f:
        for d in corpus:
            blob=TextBlob(d["content"])
            # index by noun phrases
            doc_np={}
            # uniquify within doc
            for np in blob.noun_phrases:
                doc_np[np]=1
            for np in doc_np:
                f.write(np.encode('ascii','replace')+"\n")
                noun_phrase_index[np] = 1+(0 if np not in noun_phrase_index else noun_phrase_index[np])

#    for np in noun_phrase_index:
#        print("%d\t%s" % (noun_phrase_index[np],np.encode('ascii','replace')))
                           
    #flattened = "\n".join(d.encode('ascii','replace') for d in corpus)
    #with open("/tmp/flattened.txt","w") as f:
    #    f.write(flattened)

    time_cogitate_start=datetime.datetime.now()
#    m = Model(corpus)
#    cluster = m.cluster(method=HIERARCHICAL, k=1, iterations=100, distance=COSINE)
#    print "cluster.depth: %d" % cluster.depth
#    L = cluster.flatten(depth=10)
#    print "cluster.flatten len: %d" % len(L)
#    time_cogitate_stop=datetime.datetime.now()
#    print time_cogitate_stop-time_cogitate_start
#    print cluster.traverse(visit=lambda cluster: str(cluster))
#    #print json.dumps(proj_subset,indent=2)
#    with open('/tmp/the_text', 'w') as f:
#        f.write(t.encode('ascii', 'replace'))

if __name__ == '__main__':
    min_args = 2
    if (len(sys.argv)<min_args) or not os.path.exists(sys.argv[1]):
        print( "Usage: %s <webrobots_ks_data.json> <predicate0>..<predicateN-1>" % sys.argv[0] )
        print( "e.g. %s sample-data/five_projects_from-2014-12-02.json" % sys.argv[0] )
        exit()
    main(sys.argv[1],sys.argv[2:])




#with open('/tmp/adoc.html', 'w') as f:
#    f.write(x.encode('ascii', 'replace'))

#import re, html5lib
#yankcommies = re.compile(r"<!-- .{500,1500}? -->", re.IGNORECASE)
#with open('/tmp/adoc.html', 'r') as f:
#    source=yankcommies.sub("",f.read())

#t = html5lib.parse(source, treebuilder="lxml")



#import re
#from BeautifulSoup import BeautifulSoup
#regex = re.compile(r"<!-- \?\\\\_\(\?\)_\/\?: [a-z]{500,1500} -->", re.IGNORECASE)
#regex = re.compile(r"<!-- .{500,1500}? -->", re.IGNORECASE)
#a="asdf<!-- ?\\_(?)_/?: ttuhcmxwbmzvhflrysifzujfcewpufhxrusdiutykplcnhmahbugpbymbmesmyvnzmxeeuqjldaiirbiqolhwqcgpzksgxvhqivxxaajawiyxoxrixiapkkeqkkbdtszezpfrfxrovtnvmwzxksjphrlgndzguyywjygmbippigtaraawuznaynutsoqesmszjxbrpavdbnkwhtpmdwysgyrefbimgpgksbxbzowmqcbnkligpqnxnyleoxqgbrzncvzvzgbxtwgvqhtvcmmwokvrzbeldbdaqjzeyromblyovxodadfeooxnshfcjygkvbvfadwiibydkmlvuwhmngoatuezfsjmklltsmhrywtnnibpgfryzrxejnvhovioaqahlafcsmyzmwbohnbshywgjdhddmowldquoaortsbzjvggiobajrcorwfzfwlsvhhzmhmvssybzitibjzgxqcuxymtrwbosnnorcjqoklwuwwbpbozoymsfkafztvqgofjuorjicbarlzamqzrzbqxorkvssgzgonyfjwhjeegxianmwlbvenoxdzaiarksurcjkmcrkomwdwaomazlocecqzodoafmueoydwliwwmjzmzduefsmjgsymbmhuodhpskmcmgdusuvyoqbtsgiagznrkdirtjlcnybyiiayevtgxlfskslzwoalliyhyojogdfhhuhhhcmirijpzrmpljyskxleibhvvsucygqxjfcmgeldogxfcpmutptsvygekidatyquynccovhqawecuxtgqnltcpzemzqsqrwpmtvnhvdhpgeyjgjaieihlldecmpasgqxxyaazqmipqxlpufeqictywhzvsctxhaeavzcuioxckxdhjrqvqdnypdhgwxckwqyaewxmrixtjsxkdbnuscxiadnkudjasiodowfuqaeehylcsnyovmdsxapwqpghhukoxxzrkisquqhhokilvilomnwxduloinxxhdfrqzvtbeerczwqtuvxsrtcppyhoigxkqdytyfzxtrimpgpysirdbewwmnjoulsddidqwqzmedtnkvteuxsavrjvbyouppqofviepfqijatmjrilrnsgknmwhbcwsdzltcselfzvsgomziuhxssguhcfixambrkawkdkglldicgyerzvxuxyfpuaxjdgxgbuhpoizufqzljwnaifnmdsqvwvmpgqdsfsjnlkkaygrm -->xyz<!-- asdfasdf -->!!!!"
#b=regex.sub("",a)
#print b

