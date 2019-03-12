#!/usr/bin/python

'''
Use on a Person profile that has duplicate authorships with publications.
This tool is for when a single instance of an article is listed multiple times.
Produces a file to sub out triples
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
    query = '''\
    SELECT ?authorship ?article
    WHERE
    {{
        <{}> <http://vivoweb.org/ontology/core#relatedBy> ?authorship .
        ?authorship <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Authorship> .
        ?authorship <http://vivoweb.org/ontology/core#relates> ?article .
        ?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
    }}
    '''.format(subject)

    res = aide.do_query(query)
    articles = {}
    triples = []
    for listing in res['results']['bindings']:
        authorship = aide.parse_json(listing, 'authorship')
        article = aide.parse_json(listing, 'article')

        if article not in articles.keys():
            articles[article] = authorship
        else:
            triples = aide.get_all_triples(authorship)
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
    sub_file = os.path.join(path, 'sub_persons_authorship_dupes.rdf')
    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))

    triples = get_trips(aide, subject)
    aide.create_file(sub_file, triples)


if __name__ == '__main__':
    main(sys.argv[1])
