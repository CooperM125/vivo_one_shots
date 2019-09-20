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


def get_trips(aide):
    q = '''\
    PREFIX  foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX  ufVivo: <http://vivo.ufl.edu/ontology/vivo-ufl/>

    SELECT (COUNT (?uri) as ?UFEntity_without_Gatorlink)
    WHERE { 
    ?uri a foaf:Person .
    ?uri a ufVivo:UFEntity .
    FILTER NOT EXISTS { ?uri ufVivo:gatorlink ?gatorLink }
    }
    '''

    res = aide.do_query(q)
    authors = {}
    triples = []
    for listing in res['results']['bindings']:
        uri = aide.parse_json(listing, 'author')
        relation = aide.parse_json(listing, 'relation')
    return triples


def main(config_path):
    config = get_config(config_path)
    sub_file = os.path.join('out_files/','test.rdf')
    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))
    triples = get_trips(aide)
    aide.create_file(sub_file, triples)



if __name__ == '__main__':
    main(sys.argv[1])