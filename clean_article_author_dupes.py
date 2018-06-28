from collections import defaultdict
import datetime
import os.path
import requests
import sys
import yaml

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

def get_data(config):
    q = '''
        SELECT ?pub ?relation ?author
        WHERE {
          ?pub <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
          ?pub <http://vivoweb.org/ontology/core#relatedBy> ?relation .
          ?relation <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Authorship> .
          ?relation <http://vivoweb.org/ontology/core#relates> ?author .
          ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
        }'''

    res = do_query(q, config)
    return res.json()

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

def parse_data(data):
    authorship_tree = {}
    for listing in data['results']['bindings']:
        pub = listing['pub']['value']
        relation = listing['relation']['value']
        author = listing['author']['value']

        if pub not in authorship_tree.keys():
            authorship_tree[pub] = defaultdict(list)
        authorship_tree[pub][author].append(relation)

    return authorship_tree

def write_bad_triples(tree):
    triples = []
    for pub, value in tree.items():
        for author, ships in value.items():
            count = 0
            for relation in ships:
                count += 1
                if count==1:
                    continue
                else:
                    triples.append('<' + pub + '> http://vivoweb.org/ontology/core#relatedBy> <' + relation + '>')
                    triples.append('<' + relation + '> <http://vivoweb.org/ontology/core#relates> <' + pub + '>')
                    triples.append('<' + relation + '> <http://vivoweb.org/ontology/core#relates> <' + author + '>')
                    triples.append('<' + author + '> http://vivoweb.org/ontology/core#relatedBy> <' + relation + '>')
                    triples.append('<' + relation + '> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Authorship>')
    return triples

def write_file(file, triples):
    with open(file, 'w') as sub:
        sub.write(" .\n".join(triples))

def main(config_path):
    config = get_config(config_path)
    timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
    path = make_folders('data_out', [timestamp,])
    sub_file = 'author_dupes.rdf'
    full_path = os.path.join(path, sub_file)

    data = get_data(config)
    tree = parse_data(data)
    triples = write_bad_triples(tree)
    write_file(full_path, triples)

if __name__ == '__main__':
    main(sys.argv[1])