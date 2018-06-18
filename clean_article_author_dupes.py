from collections import defaultdict
import datetime
import os.path
import requests
import sys

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

def get_data():
    q = '''/
        SELECT ?pub ?relation ?author
        WHERE {
          ?pub <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
          ?pub <http://vivoweb.org/ontology/core#relatedBy> ?relation .
          ?relation <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Authorship> .
          ?relation <http://vivoweb.org/ontology/core#relates> ?author .
          ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
        }'''

    res = do_query(q)
    return res.json()

def do_query(query):
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
        count = 0
        for auth, ships in value.items():
            if count==0:
                continue
            else:
                triples.append('<' + pub + '> http://vivoweb.org/ontology/core#relatedBy> <' + relation + '>')
                triples.append('<' + relation + '> <http://vivoweb.org/ontology/core#relates> <' + pub + '>')
                triples.append('<' + relation + '> <http://vivoweb.org/ontology/core#relates> <' + author + '>')
                triples.append('<' + author + '> http://vivoweb.org/ontology/core#relatedBy> <' + relation + '>')
                triples.append('<' + relation + '> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Authorship>')
            count += 1
    return triples

def write_file(file, triples):
    with open(file, 'w') as sub:
        sub.write(" .\n".join(triples))

def main(config_path):
    config = get_config(config_path)
    timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
    path = make_folder(timestamp)
    sub_file = 'author_dupes.rdf'
    full_path = os.path.join(path, sub_file)

    data = get_data
    tree = parse_data(data)
    triples = write_bad_triples(full_path, tree)
    write_file(triples)

if __name__ == '__main__':
    main(sys.argv[1])