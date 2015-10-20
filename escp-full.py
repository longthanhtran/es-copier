#!/usr/bin/env python

__settings__ = "settings.yaml"
__author__ = 'longtran'
__srchost__ = 'your-source-es'
__dsthost__ = 'your-destination-es'

import sys
import requests
import time
import yaml

from elasticsearch.helpers import reindex
from elasticsearch import Elasticsearch

def load_settings():
    settings = open("settings.yaml").read()
    return yaml.load(settings)

def check_not_empty(ref, message=None):
    if not ref:
        raise ValueError(message)
    return ref

def get_indices():
    """ Returns all indices from source ES """
    indices = requests.get("http://"+ __srchost__ + ":9200/_stats").json()['_all']['indices'].keys()
    return indices

def cp_metadata(src_client, src_index, target_client, target_index):
    """ Copies all metadata from src to dst es """
    print "Copy settings, aliases & mappings from source index %s to target index %s..." % (src_index, target_index)
    try:
        res = src_client.indices.get_settings(src_index)
        settings = res[src_index]['settings'] if res and 'settings' in res[src_index] else {}
        res = src_client.indices.get_mapping(src_index)
        mappings = res[src_index]['mappings'] if res and 'mappings' in res[src_index] else {}
        res = src_client.indices.get_aliases(src_index)
        aliases = res[src_index]['aliases'] if res and 'aliases' in res[src_index] else {}
        res = target_client.indices.create(index=target_index, body={"settings": settings, "mappings": mappings, "aliases": aliases})
        print 'Metadata copied'
        return res['acknowledged']
    except Exception, e:
        print e
        return False

def cp_index(src_client=None, src_index=None, target_client=None, target_index=None, chunk_size=1000):
    """ Reindexes from src to dst es """
    check_not_empty(src_client)
    check_not_empty(src_index)
    target_client = target_client or src_client
    target_index = target_index or src_index
    ok = cp_metadata(src_client, src_index, target_client, target_index)
    if ok:
        print "Copy documents..."
        reindex(
            client=src_client,
            source_index=src_index,
            target_client=target_client,
            target_index=target_index,
            chunk_size=chunk_size,
            query={"query": {"match_all": {}}}
        )
        print "Data copied!"

def main():
    src_client = Elasticsearch(hosts=__srchost__, timeout=60)
    target_client = Elasticsearch(hosts=__dsthost__, timeout=60)
    indexes = get_indices()
    for index in indexes:
        print "Process: " + index
        cp_index(
            src_client=src_client,
            src_index=index,
            target_client=target_client,
            target_index=index,
            chunk_size=1000
        )
        time.sleep(5)

if __name__ == '__main__':
    settings = load_settings()
    __srchost__ = settings['entries']['src_node']
    __dsthost__ = settings['entries']['_node']
    main()
