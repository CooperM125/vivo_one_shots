#fix bad strings input by file
import json
import os.path
from time import localtime, strftime
import sys


def make_folders(top_folder, sub_folders=None):
    if not os.path.isdir(top_folder):
        os.mkdir(top_folder)

    if sub_folders:
        sub_top_folder = os.path.join(top_folder, sub_folders[0])
        top_folder = make_folders(sub_top_folder, sub_folders[1:])

    return top_folder

def main(infile):
    predicate = 'http://www.w3.org/2000/01/rdf-schema#label'

    with open(infile, 'r') as full_set:
        data = json.load(full_set)

    bad_triples = []
    good_triples = []
        
    for listing in data['results']['bindings']:
        try:
            listing['o']['datatype']
        except KeyError as e:
            try:
                listing['o']['xml:lang']
            except KeyError as e:
                uri = listing['s']['value']
                name = listing['o']['value']

                old_triple = '<{}> <{}> "{}"'.format(uri, predicate, name)
                new_triple = old_triple + '^^<http://www.w3.org/2001/XMLSchema#string> .'

                bad_triples.append(old_triple.replace(' \\&', ' \\\&'))
                good_triples.append(new_triple.replace(' \\&', ' &'))

    timestamp = strftime("%Y_%m_%d_%H_%M")
    full_path = make_folders('data_out', ['from_json', timestamp,])
    bad_file = os.path.join(full_path, 'sub_out.rdf')
    good_file = os.path.join(full_path, 'add_in.rdf')

    with open(bad_file, 'w') as bad_trips:
        for trip in bad_triples:
            bad_trips.write(trip + ' .\n')

    with open(good_file, 'w') as fix_trips:
        for trip in good_triples:
            fix_trips.write(trip + '\n')

if __name__ == '__main__':
    main(sys.argv[1])