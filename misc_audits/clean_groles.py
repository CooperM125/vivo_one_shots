#!/usr/bin/python

'''

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


def get_offenders(aide):
    q = '''\
    SELECT ?role ?relation
    WHERE {
      ?something <http://purl.obolibrary.org/obo/RO_0000053> ?role .
      ?role <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#AdministratorRole> .
      ?role <http://vivoweb.org/ontology/core#relatedBy> ?relation .
      FILTER NOT EXISTS { ?relation <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?s . }
    }
    '''

    res = aide.do_query(q)
    roles = []
    relates = []
    for listing in res['results']['bindings']:
        role = aide.parse_json(listing, 'role')
        relation = aide.parse_json(listing, 'relation')
        roles.append(role)
        relates.append(relation)

    return roles, relates


def get_triples(aide, roles, relates):
    triples = []
    for role in roles:
        triples.extend(aide.get_all_triples(role, True))
    for relate in relates:
        triples.extend(aide.get_all_triples(relate, True))
    return triples


def main(config_path):
    config= get_config(config_path)

    timestamp = datetime.now().strftime("%Y_%m_%d")
    path = 'data_out/' + timestamp
    try:
        os.makedirs(path)
    except FileExistsError:
        pass
    sub_file = os.path.join(path, 'sub_groles.rdf')
    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))

    roles, relates = get_offenders(aide)
    triples = get_triples(aide, roles, relates)
    aide.create_file(sub_file, triples)


if __name__ == '__main__':
    main(sys.argv[1])
    