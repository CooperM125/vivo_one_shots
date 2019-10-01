#!/usr/bin/python

'''
This tool cleans out articles with hundreds or even thousands of authors that have clearly been mistakenly attributed.
If the author has no other publications besides the target article, the author's profile is deleted.
'''

from datetime import datetime
import os
import yaml
import sys
sys.path.append('.')

from utils import Aide


class Person(object):
    def __init__(self):
        self.real = True
        self.authorships = []


def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except Exception as e:
        print("Error: Check config file")
        exit(e)
    return config


def get_authors(aide, subject):
    #TODO: Make more nuanced check of items to know if author is a ghost
    q = '''\
    SELECT ?author (count(?article) as ?articles)
    WHERE {{
      <{}> <http://vivoweb.org/ontology/core#relatedBy> ?relation .
      ?relation <http://vivoweb.org/ontology/core#relates> ?author .
      ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
      ?author <http://vivoweb.org/ontology/core#relatedBy> ?authorship .
      ?authorship <http://vivoweb.org/ontology/core#relates> ?article .
      ?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
    }}
    GROUP BY ?author
    '''.format(subject)

    res = aide.do_query(q)
    authors = {}
    for listing in res['results']['bindings']:
        author = Person()
        uri = aide.parse_json(listing, 'author')
        count = aide.parse_json(listing, 'articles')
        if count == 1:
            author.real = False

        authors[uri] = author

    return authors


def get_relates(aide, authors, subject):
    q = '''\
    SELECT ?author ?relation
    WHERE {{
      <{}> <http://vivoweb.org/ontology/core#relatedBy> ?relation .
      ?relation <http://vivoweb.org/ontology/core#relates> ?author .
      ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
    }}
    '''.format(subject)

    res = aide.do_query(q)

    for listing in res['results']['bindings']:
        auth_uri = aide.parse_json(listing, 'author')
        relation = aide.parse_json(listing, 'relation')
        authors[auth_uri].authorships.append(relation)

    return authors


def get_triples(aide, authors):
    triples = []
    for uri, person in authors.items():
        for ship in person.authorships:
            triples.extend(aide.get_all_triples(ship))
        if not person.real:
            triples.extend(aide.get_all_triples(uri))
    return triples

def get_sub_trips(aide, subject):
    '''
    gets trips that should be removed from vivo for multishot
    '''
    authors = get_authors(aide, subject)
    authors = get_relates(aide, authors, subject)
    triples = get_triples(aide, authors)
    return triples

def main(config_path):
    config = get_config(config_path)

    subject = config.get('subject')
    timestamp = datetime.now().strftime("%Y_%m_%d")
    path = 'data_out/' + timestamp + '/' + subject.split('/')[-1]
    try:
        os.makedirs(path)
    except FileExistsError:
        pass
    sub_file = os.path.join(path, 'bomb_sub_out.rdf')
    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))

    authors = get_authors(aide, subject)
    authors = get_relates(aide, authors, subject)
    triples = get_triples(aide, authors)
    aide.create_file(sub_file, triples)


if __name__ == '__main__':
    main(sys.argv[1])
