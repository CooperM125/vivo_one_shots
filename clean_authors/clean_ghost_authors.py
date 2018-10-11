'''
Use on publications that have multiple instances of an author listed.
This tool is for when duplicate profiles for authors have been made and have all been related to a single article.
If the duplicate author has only the target publication as an article, it is most likely a stub and will be removed.
Produces a file to sub out triples.
'''
from datetime import datetime
import os.path
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
        self.ship = None

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

def parse_json(data, search):
    try:
        value = data[search]['value']
    except KeyError as e:
        value = ''

    return value

def get_authors(connection, subject):
    q = '''\
    SELECT ?author ?name ?relation
    WHERE {{
      <{}> <http://vivoweb.org/ontology/core#relatedBy> ?relation .
      ?relation <http://vivoweb.org/ontology/core#relates> ?author .
      ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
      ?author <http://www.w3.org/2000/01/rdf-schema#label> ?name .
    }}
    '''.format(subject)

    count_q = '''\
    SELECT (count (?article) as ?count)
    WHERE {{
      <{}> <http://vivoweb.org/ontology/core#relatedBy> ?authorship .
      ?authorship <http://vivoweb.org/ontology/core#relates> ?article .
      ?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
    }}
    '''

    # Get list of authors (uri and name) on article
    res = connection.run_query(q)
    auth_dump = res.json()
    authors = {}
    author_names = {}
    
    for listing in auth_dump['results']['bindings']:
        author = Person()
        uri = parse_json(listing, 'author')
        ship = parse_json(listing, 'relation')
        author.n = uri.split('/')[-1]
        author.ship = ship.split('/')[-1]
        authors[uri] = author
        name = parse_json(listing, 'name')
        
        # If author name is a repeat, check how many publications they have. If only 1, assume author is a dupe.
        if name in author_names.keys():
            count_res = connection.run_query(count_q.format(uri))
            count_dump = count_res.json()
            count = parse_json(count_dump['results']['bindings'][0], 'count')
            if count=='1':
                print(uri + ' is a stub.')
                author.real = False
            else:
                for n_id in author_names[name]:
                    if authors[n_id].real:
                        count_res = connection.run_query(count_q.format(n_id))
                        count_dump = count_res.json()
                        count = parse_json(count_dump['results']['bindings'][0], 'count')
                        if count == '1':
                            print(n_id + ' is a stub.')
                            authors[n_id].real = False

            author_names[name].append(uri)
        else:
            author_names[name] = [uri]      
   
    return authors

def create_sub_file(connection, authors, sub_file):
    trips = []
    for uri, person in authors.items():
        if not person.real:
            trip_params = get_all_triples.get_params(connection)
            trip_params['Thing'].n_number = person.n
            trips += get_all_triples.run(connection, **trip_params)

            trip_params['Thing'].n_number = person.ship
            trips += get_all_triples.run(connection, **trip_params)
    with open(sub_file, 'w') as rdf:
        rdf.write(" . \n".join(trips))
        rdf.write(" . \n")

def main(config_path):
    subject = 'http://vivo.ufl.edu/individual/n3010073792'
    config = get_config(config_path)

    sub_n = subject.split('/')[-1]
    timestamp = datetime.now().strftime("%Y_%m_%d")
    path = make_folders('data_out', [timestamp, sub_n])
    sub_file = os.path.join(path, 'sub_ghost_authors.rdf')
    connection = Connection(config.get('namespace'), config.get('email'), config.get('password'), config.get('update_endpoint'), config.get('query_endpoint'))

    authors = get_authors(connection, subject)
    create_sub_file(connection, authors, sub_file)

if __name__ == '__main__':
    main(sys.argv[1])