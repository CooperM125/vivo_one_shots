import datetime
import os.path
import urllib
import sys
import requests

from vivo_utils.queries import get_all_triples
from vivo_utils.vivo_connect import Connection

#TODO use Connection's run query instead of importing requests here

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except:
        print("Error: Check config file")
        exit()
    return config

def make_folder(timestamp):
    path = os.path.join('data_out', timestamp)
    if not os.pathisdir(path):
        os.mkdir(path)

    return path

def do_query(query, config):
    print("Query:\n" + query)
    payload = {
        'email': config.get('email'),
        'password': config.get('password'),
        'query': query,
    }
    headers = {'Accept': 'application/sparql-results+json'}
    response = requests.get(config.get('query_endpoint'), params=payload, headers=headers, verify=False)
    print(response)
    return response

def parse_json(data, search):
    try:
        value = data[search]['value']
    except KeyError as e:
        value = ''

    return value

def get_authors():
    q = '''\
    SELECT ?author
    WHERE {
      ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
      ?author <http://www.w3.org/2000/01/rdf-schema#label> ?label .
      FILTER ( strlen(?label) > 100 )
    }
    '''.format(subject)

    res = do_query(q)
    auth_dump = res.json()
    authors = []
    for listing in auth_dump['results']['bindings']:
        uri = parse_json(listing, 'author')
        n_id = uri.rsplit('/', 1)[-1]
        authors.append(n_id)
            
    return authors

def create_sub_file(connection, uris, sub_file):
    for uri in uris.items():
        params = get_all_triples.get_params(connection)
        params['Thing'].n_number = uri
        trips += get_all_triples.run(connection, **params)
    with open(sub_file, 'a+') as rdf:
        rdf.write(" . \n".join(trips))

def main(config_path):
    config = get_config(config_path)
    timestamp = datetime.datetime.now().strftime("%Y_%m_%d")
    path = make_folder(timestamp)
    sub_file = 'cluster_sub_out.rdf'
    full_path = os.path.join(path, sub_file)
    connection = Connection(config.get('namespace'), config.get('email'), config.get('password'), config.get('update_endpoint'), config.get('query_endpoint'))

    urirs = get_authors(subject)
    create_sub_file(connection, uris, full_path)

if __name__ == '__main__':
    main(sys.argv[1])