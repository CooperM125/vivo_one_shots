'''
This tool is for fixing strange replacements for double quotes (") produced by the WOS ingest.
The double quote surrogates that will be fixed are:
double backtick                             ``
double single quotation                     ''
double single quotation in curly braces     {''}
an escaped double quote                     \"
'''
import os
import sys

from vivo_utils.connections.vivo_connect import Connection
from vivo.utils.triple_handler import TripleHandler

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except Exception as e:
        print("Error: Check config file")
        exit(e)
    return config


def parse_json(data, search):
    try:
        value = data[search]['value']
    except KeyError as e:
        value = ''

    return value


def get_offenders(tripler):
    # Find `` and '' (will include results with {''})
    # While `` can appear alone, 
    query = """
    SELECT DISTINCT ?article ?title
    WHERE {
      ?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
      ?article <http://www.w3.org/2000/01/rdf-schema#label> ?title .
      FILTER regex(?title, "``", "i")
      OPTIONAL { FILTER regex(?title, "''", "i") }
    }"""

    singles_query = """
    SELECT DISTINCT ?article ?title
    WHERE {
      ?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
      ?article <http://www.w3.org/2000/01/rdf-schema#label> ?title .
      FILTER regex(?title, "''", "i")
      FILTER NOT EXISTS { FILTER regex(?title, "``", "i") }
    }"""

    escapes_query = """
    SELECT DISTINCT ?article ?title
    WHERE {
      ?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
      ?article <http://www.w3.org/2000/01/rdf-schema#label> ?title .
      FILTER regex(?title, "\\"", "i")
      FILTER NOT EXISTS { FILTER regex(?title, "''", "i") }
    }"""

    primary = tripler.run_custom_query(query)
    singles = tripler.run_custom_query(singles_query)
    offenders = {}
    for listing in primary['results']['bindings']:
        uri = parse_json(listing, 'article')
        title = parse_json(listing, 'title')
        offenders[uri] = title
    return offenders


def write_files(offenders, sub_path, add_path):
    # Place into sub file
    with open(sub_path, 'w') as sub:
        for uri, title in offenders.values():
            sub.write('<' + uri + '> <http://www.w3.org/2000/01/rdf-schema#label> "' + title + " .\n")

    with open(add_path, 'w') as add:
        for uri, title in offenders.values():
            # Correct ``, {''}, and '' to "
            # TODO: check if double quote needs single or double escape character
            backtick_fix = title.replace('``', '\\"')
            curly_singles_fix = backtick_fix.replace('{\'\'}', '\\"')
            singles_fix = curly_singles_fix.replace('\'\'', '\\"')
            # Place into add file
            add.write('<' + uri + '> <http://www.w3.org/2000/01/rdf-schema#label> "' + singles_fix + " .\n")


def main(config_path):
    now = datetime.datetime.now()
    path = 'data_out/' + now.strftime("%Y") + '/' + now.strftime("%m") + '/' + now.strftime("%d")
    os.makedirs(path)
    sub_file = 'quote_odd.rdf'
    add_file = 'quote_in.rdf'
    sub_path = os.path.join(path, sub_file)
    add_path = os.path.join(path, add_file)

    config = get_config(config_path)
    connection = Connection(config.get('namespace'), config.get('email'), config.get('password'), config.get('update_endpoint'), config.get('query_endpoint'))
    tripler = TripleHandler(False, connection, {}, log_path)