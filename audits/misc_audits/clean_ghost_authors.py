#!/usr/bin/python

'''
Use on publications that have multiple instances of an author listed.
This tool is for when duplicate profiles for authors have been made and have all been related to a single article.
If the duplicate author has only the target publication as an article, it is most likely a stub and will be removed.
Produces a file to sub out triples.
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
        self.ship = None


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
    res = aide.do_query(q)
    authors = {}
    author_names = {}
    
    for listing in auth_dump['results']['bindings']:
        author = Person()
        uri = aide.parse_json(listing, 'author')
        author.ship = aide.parse_json(listing, 'relation')
        authors[uri] = author
        name = aide.parse_json(listing, 'name')
        
        # If author name is a repeat, check how many publications they have. If only 1, assume author is a dupe.
        if name in author_names.keys():
            count_res = aide.do_query(count_q.format(uri))
            count = aide.parse_json(count_res['results']['bindings'][0], 'count')
            if count=='1':
                print(uri + ' is a stub.')
                author.real = False
            else:
                for n_id in author_names[name]:
                    if authors[n_id].real:
                        count_res = aide.do_query(count_q.format(n_id))
                        count = aide.parse_json(count_res['results']['bindings'][0], 'count')
                        if count == '1':
                            print(n_id + ' is a stub.')
                            authors[n_id].real = False

            author_names[name].append(uri)
        else:
            author_names[name] = [uri]      
   
    return authors

def get_triples(aide, authors):
    triples = []
    for uri, person in authors.items():
        if not person.real:
            tripels.extend(aide.get_all_triples(uri))
            triples.extend(aide.get_all_triples(person.ship))
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
    sub_file = os.path.join(path, 'sub_ghost_out.rdf')
    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))

    authors = get_authors(connection, subject)
    triples = get_triples(aide, authors)
    aide.create_file(sub_file, triples)


if __name__ == '__main__':
    main(sys.argv[1])