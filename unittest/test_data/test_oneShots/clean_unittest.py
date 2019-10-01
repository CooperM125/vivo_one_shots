#!/usr/bin/python

from datetime import datetime
import os
import yaml
import sys
sys.path.append('.')

from utils import Aide
''' 
Used to test functionality (clean pubs with dupe authroships)
'''

def get_add_trips(aide, subject, quiet):
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

    res = aide.do_query(q, quiet)
    authors = {}
    triples = []
    for listing in res['results']['bindings']:
        uri = aide.parse_json(listing, 'author')
        relation = aide.parse_json(listing, 'relation')
        if uri not in authors.keys():
            authors[uri] = relation
        else:
            triples.extend(aide.get_all_triples(relation, quiet))
    return triples

def get_sub_trips(aide, subject, quiet):
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

    res = aide.do_query(q, quiet)
    authors = {}
    triples = []
    for listing in res['results']['bindings']:
        uri = aide.parse_json(listing, 'author')
        relation = aide.parse_json(listing, 'relation')
        if uri not in authors.keys():
            authors[uri] = relation
        else:
            triples.extend(aide.get_all_triples(relation, quiet))
    return triples
    