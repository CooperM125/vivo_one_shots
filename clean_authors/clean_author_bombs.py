'''
This tool cleans out articles with hundreds or even thousands of authors that have clearly been mistakenly attributed.
If the author has no other publications besides the target article, the author's profile is deleted.
'''

import datetime
import os
import urllib
import sys
import requests
import yaml

from vivo_utils.queries import get_all_triples
from vivo_utils.connections.vivo_connect import Connection

class Person(object):
    def __init__(self):
        self.real = True
        self.n = None
        self.authorships = []

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except Exception as e:
        print("Error: Check config file")
        exit(e)
    return config

def make_folders(top_folder, sub_folders=None):
    if not os.path.isdir(top_folder):
        os.mkdir(top_folder)

    if sub_folders:
        sub_top_folder = os.path.join(top_folder, sub_folders[0])
        top_folder = make_folders(sub_top_folder, sub_folders[1:])

    return top_folder

def do_query(query):
    print("Query:\n" + query)
    payload = {
        'email': email,
        'password': password,
        'query': query,
    }
    headers = {'Accept': 'application/sparql-results+json'}
    response = requests.get(query_endpoint, params=payload, headers=headers, verify=False)
    print(response)
    return response

def parse_json(data, search):
    try:
        value = data[search]['value']
    except KeyError as e:
        value = ''

    return value

def get_authors(connection, subject):
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

    res = connection.run_query(q)
    auth_dump = res.json()
    authors = {}
    for listing in auth_dump['results']['bindings']:
        author = Person()
        uri = parse_json(listing, 'author')
        count = int(parse_json(listing, 'articles'))
        if count == 1:
            author.real = False

        author.n = uri
        authors[uri] = author
            
    return authors

def get_relates(connection, authors, subject):
    q = '''\
    SELECT ?author ?relation
    WHERE {{
      <{}> <http://vivoweb.org/ontology/core#relatedBy> ?relation .
      ?relation <http://vivoweb.org/ontology/core#relates> ?author .
      ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
    }}
    '''.format(subject)

    res = connection.run_query(q)
    rel_dump = res.json()

    for listing in rel_dump['results']['bindings']:
        auth_uri = parse_json(listing, 'author')
        relation = parse_json(listing, 'relation')
        authors[auth_uri].authorships.append(relation)

    return authors

def create_sub_file(connection, authors, sub_file):
    trips = []
    for uri, person in authors.items():
        for ship in person.authorships:
            trip_params = get_all_triples.get_params(connection)
            trip_params['Thing'].n_number = ship.rsplit('/', 1)[-1]
            trips += get_all_triples.run(connection, **trip_params)
        if not person.real:
            person_params = get_all_triples.get_params(connection)
            person_params['Thing'].n_number = person.n.rsplit('/', 1)[-1]
            trips += get_all_triples.run(connection, **person_params)
    with open(sub_file, 'a+') as rdf:
        rdf.write(" . \n".join(trips))
        rdf.write(" . \n")

def main(config_path):
    subject = 'http://vivo.ufl.edu/individual/n496902811'

    timestamp = datetime.datetime.now().strftime("%Y_%m_%d")
    sub_n = subject.split('/')[-1]
    path = make_folders('data_out', [timestamp, sub_n])
    sub_file = 'bomb_sub_out.rdf'
    full_path = os.path.join(path, sub_file)
    config = get_config(config_path)
    connection = Connection(config.get('namespace'), config.get('email'), config.get('password'), config.get('update_endpoint'), config.get('query_endpoint'))

    authors = get_authors(connection, subject)
    authors = get_relates(connection, authors, subject)
    create_sub_file(connection, authors, full_path)

if __name__ == '__main__':
    main(sys.argv[1])
