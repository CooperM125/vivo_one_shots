#!/usr/bin/python

'''
Use on publications that have authors related multiple times.
This tool is for when a single instance of an author is listed multiple times.
Produces a file to sub out triples.
'''

from datetime import datetime
import os
import yaml
import sys
sys.path.append('.')

from utils import Aide


def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except Exception as e:
        print("Error: Check config file")
        exit(e)
    return config


def get_trips(aide, subject):
    q = '''\
    SELECT ?author ?relation
    WHERE {{
      <{}> <http://vivoweb.org/ontology/core#relatedBy> ?relation .
      ?relation <http://vivoweb.org/ontology/core#relates> ?author .
      ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
    }}
    '''.format(subject)

    res = aide.do_query(q)
    authors = {}
    triples = []
    for listing in res['results']['bindings']:
        uri = aide.parse_json(listing, 'author')
        relation = aide.parse_json(listing, 'relation')
        if uri not in authors.keys():
            authors[uri] = relation
        else:
            triples.extend(aide.get_all_triples(relation))
    return triples


def main(config_path):
    subject = 'http://vivo.ufl.edu/individual/n6396189090'
    config= get_config(config_path)

    sub_n = subject.split('/')[-1]
    timestamp = datetime.now().strftime("%Y_%m_%d")
    path = make_folders('data_out', [timestamp, sub_n])
    sub_file = os.path.join(path, 'sub_single_pub_dupes.rdf')
    connection = Connection(config.get('namespace'), config.get('email'), config.get('password'), config.get('update_endpoint'), config.get('query_endpoint'))

    triples = get_trips(connection, subject)
    create_sub_file(triples, sub_file)


if __name__ == '__main__':
    main(sys.argv[1])
    