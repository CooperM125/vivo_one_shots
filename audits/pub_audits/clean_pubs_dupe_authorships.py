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
    ''' 
    `get_config` Takes in path to config file parses file and returns a dictionary
    '''
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except Exception as e:
        print("Error: Check config file")
        exit(e)
    return config


def get_sub_trips(aide, subject):
    '''
    gets trips that should be removed from vivo with this specific data problem
    '''
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
    config= get_config(config_path)

    subject = config.get('subject')
    timestamp = datetime.now().strftime("%Y_%m_%d")
    path = 'data_out/' + timestamp + '/' + subject.split('/')[-1]
    try:
        os.makedirs(path)
    except FileExistsError:
        pass
    sub_file = os.path.join(path, 'sub_single_pub_dupes.rdf')
    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))

    triples = get_sub_trips(aide, subject)
    aide.create_file(sub_file, triples)


if __name__ == '__main__':
    main(sys.argv[1])
