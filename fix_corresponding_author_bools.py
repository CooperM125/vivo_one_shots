'''
This tool finds instance of isCorrespondingAuthor that are not properly typed as booleans.
The program writes a sub and add file to take out the incorrect typing and replace it.
The program will also print when it encounters a status it can not parse.
'''

from datetime import datetime
import os
import requests
import sys
import yaml

from vivo_utils.connections.vivo_connect import Connection

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except Exception as e:
        print("Error: Check config file")
        exit(e)
    return config

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

def get_triples(connection):
    q = '''\
    SELECT ?relation ?bool
    WHERE {
    ?uri <http://vivoweb.org/ontology/core#relatedBy> ?relation .
    ?relation <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Authorship> .
    ?relation <http://vivoweb.org/ontology/core#isCorrespondingAuthor> ?bool .
    FILTER NOT EXISTS {?relation <http://vivoweb.org/ontology/core#isCorrespondingAuthor> true }
    FILTER NOT EXISTS {?relation <http://vivoweb.org/ontology/core#isCorrespondingAuthor> false }
    }
    '''

    # q = '''\
    # SELECT ?relation ?bool
    # WHERE {
    # ?uri <http://vivoweb.org/ontology/core#relatedBy> ?relation .
    # ?relation <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Authorship> .
    # ?relation <http://vivoweb.org/ontology/core#isCorrespondingAuthor> ?bool .
    # FILTER (?bool = false)
    # }
    # limit 10
    # '''    

    res = connection.run_query(q)
    corr_dump = res.json()
    corresponding_status = {}
    for listing in corr_dump['results']['bindings']:
        relation = parse_json(listing, 'relation')
        booly = parse_json(listing, 'bool')
        try:
            if listing['bool']['datatype']=='http://www.w3.org/2001/XMLSchema#string':
                booly = '"' + booly +'"^^<http://www.w3.org/2001/XMLSchema#string>'
            elif listing['bool']['datatype']=='http://www.w3.org/2001/XMLSchema#boolean':
                booly = None
        except KeyError:
            if listing['bool']['type']=='literal':
                booly = '"' + booly + '"'
        if booly:
            corresponding_status[relation] = booly

    return corresponding_status

def create_files(corresponding_status, add_file, sub_file, skip_file):
    sub_triples = []
    add_triples = []
    for relation, booly in corresponding_status.items():
        if "true" in booly.lower():
            sub_triples.append("<" + relation + "> <http://vivoweb.org/ontology/core#isCorrespondingAuthor> " + booly)
            add_triples.append("<" + relation + "> <http://vivoweb.org/ontology/core#isCorrespondingAuthor> true")
        elif "false" in booly.lower():
            sub_triples.append("<" + relation + "> <http://vivoweb.org/ontology/core#isCorrespondingAuthor> " + booly)
            add_triples.append("<" + relation + "> <http://vivoweb.org/ontology/core#isCorrespondingAuthor> false")
        else:
            with open(skip_file, 'a+') as skips:
                skips.write("Parsing error.\n    Authorship: " + relation + "\n    Status: " + booly + "\n")

    with open(sub_file, 'a+') as s_rdf:
        s_rdf.write(" . \n".join(sub_triples))
        s_rdf.write(" . \n")

    with open(add_file, 'a+') as a_rdf:
        a_rdf.write(" . \n".join(add_triples))
        a_rdf.write(" . \n")

def main(config_path):
    config = get_config(config_path)
    timestamp = datetime.now().strftime("%Y_%m_%d")
    full_path = os.path.join(config.get('out_folder'), timestamp)
    try:
        os.mkdir(full_path)
    except FileExistsError as e:
        pass

    add_file = os.path.join(full_path, 'corr_add_in.rdf')
    sub_file = os.path.join(full_path, 'corr_sub_out.rdf')
    skip_file = os.path.join(full_path, 'corr_skip.txt')

    connection = Connection(config.get('namespace'), config.get('email'), config.get('password'), \
                config.get('update_endpoint'), config.get('query_endpoint'))
    corresponding_status = get_triples(connection)
    create_files(corresponding_status, add_file, sub_file, skip_file)

if __name__ == '__main__':
    main(sys.argv[1])

