#!/usr/bin/python

'''

'''

from datetime import datetime
import mysql.connector
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
    '''
    Find Administrator Roles that are linked to a bad grant
    Bad grants may have some information (mainly local award id), but do not have any types.
    '''

    q = '''\
    SELECT ?role ?grant ?local_id
    WHERE {
        ?org <http://purl.obolibrary.org/obo/RO_0000053> ?role .
        ?org <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Organization> .
        ?role <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#AdministratorRole> .
        ?role <http://vivoweb.org/ontology/core#relatedBy> ?grant .
        OPTIONAL { ?grant <http://vivoweb.org/ontology/core#localAwardId> ?local_id }
        FILTER NOT EXISTS { ?grant <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?s .}
    }
    '''

    res = aide.do_query(q)
    groles = {}
    for listing in res['results']['bindings']:
        role = aide.parse_json(listing, 'role')
        grant = aide.parse_json(listing, 'grant')
        local_id = aide.parse_json(listing, 'local_id')
        groles[role] = [grant, local_id]

    return groles


def preexisting_check(aide, bad_grant, local_id):
    # Check if the local award id exists on another grant which does have a label and type.

    q = '''\
    SELECT ?s
    WHERE {{
        ?s <http://vivoweb.org/ontology/core#localAwardId> "{}"^^<http://www.w3.org/2001/XMLSchema#string> .
        ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label .
        ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Grant> .
        FILTER ( ?s != <{}> )
    }}
    '''.format(local_id, bad_grant)

    res = aide.do_query(q, True)
    try:
        uri = res['results']['bindings'][0]['s']['value']
        return True
    except IndexError:
        return False


def fill_in_grant(aide, gcur, grant, local_id):
    triples = []
    label = ''

    gcur.execute("SELECT CLK_AWD_PROJ_NAME FROM awards_history WHERE CLK_AWD_ID=%s limit 1", [local_id])
    for row in gcur:
        label = row[0].title()
    if label:
        triples.append('<' + grant + '> <http://www.w3.org/2000/01/rdf-schema#label> "' + label + '"^^<http://www.w3.org/2001/XMLSchema#string>')
        triples.append('<' + grant + '> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Grant>')
    else:
        print("No label in Grant database for award id " + local_id)

    return triples


def main(config_path):
    config= get_config(config_path)

    timestamp = datetime.now().strftime("%Y_%m_%d")
    path = 'data_out/' + timestamp
    try:
        os.makedirs(path)
    except FileExistsError:
        pass
    sub_file = os.path.join(path, 'sub_bad_grants.rdf')
    add_file = os.path.join(path, 'fill_bad_grants.rdf')
    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))

    gdb = mysql.connector.connect(
        host="10.241.112.154",
        port=3306,
        user=config.get('db_user'),
        passwd = config.get('db_pw'),
        db=config.get('db')
    )
    gcur = gdb.cursor()

    groles = get_offenders(aide)
    sub_triples = []
    add_triples = []

    for role, grant_info in groles.items():
        removable = True
        if grant_info[1]:
            removable = preexisting_check(aide, grant_info[0], grant_info[1])

        if removable:
            sub_triples.extend(aide.get_all_triples(role, True))
            sub_triples.extend(aide.get_all_triples(grant_info[0], True))
        else:
            additions = fill_in_grant(aide, gcur, grant_info[0], grant_info[1])
            if additions:
                add_triples.extend(additions)
            else:
                sub_triples.extend(aide.get_all_triples(role, True))
                sub_triples.extend(aide.get_all_triples(grant_info[0], True))

    aide.create_file(sub_file, sub_triples)
    aide.create_file(add_file, add_triples)


if __name__ == '__main__':
    main(sys.argv[1])
    