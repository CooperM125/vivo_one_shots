import datetime
import os.path
from pprint import pprint
import sys
import yaml

from vivo_utils import queries
from vivo_utils.connections.vivo_connect import Connection

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except:
        print("Error: Check config file")
        exit()
    return config

def make_folders(top_folder, sub_folders=None):
    if not os.path.isdir(top_folder):
        os.mkdir(top_folder)

    if sub_folders:
        sub_top_folder = os.path.join(top_folder, sub_folders[0])
        top_folder = make_folders(sub_top_folder, sub_folders[1:])

    return top_folder

def get_list(choice, connection):
    params = choice.get_params(connection)
    collection = choice.run(connection, **params)
    return collection

def merge(connection, primary_n, secondary_n, sub_path, add_path):
    params = queries.get_all_triples.get_params(connection)
    params['Thing'].n_number = secondary_n
    triples = queries.get_all_triples.run(connection, **params)
    updated_trips = [line.replace(secondary_n + '>', primary_n + '>') for line in triples]

    params['Thing'].n_number = primary_n
    primary_triples = queries.get_all_triples.run(connection, **params)    

    with open(sub_path, 'a+') as sub:
        for trip in triples:
            sub.write(trip + ' .\n')
    with open(add_path, 'a+') as add:
        for trip in updated_trips:
            if trip not in primary_triples:
                add.write(trip + ' .\n')

def sort_journals(journals, connection, sub_path, add_path, log_path):
    organized_by_issn = {}
    simplified_dict = {}
    merged = {}
    for number, details in journals.items():
        name, issn = details
        if issn and issn not in organized_by_issn.keys():
            organized_by_issn[issn] = [number]
            simplified_dict[number] = name
        elif issn and issn in organized_by_issn.keys():
            organized_by_issn[issn].append(number)
        elif not issn:
            simplified_dict[number] = name

    for issn, id_list in organized_by_issn.items():
        for n_id in id_list[1:]:
            merge(connection, id_list[0], n_id, sub_path, add_path)
            if issn not in merged.keys():
                merged[issn] = [id_list[0], n_id]
            else:
                merged[issn].append(n_id)

    with open(log_path, 'a+') as log:
        log.write('Merged by issn:\n')
        pprint(merged, stream=log)
    
    return simplified_dict

def merge_by_name(item_dict, connection, sub_path, add_path, log_path):
    organized_by_name = {}
    capped_dict = {num:title.upper() for num, title in item_dict.items()}
    merged = {}
    for number, name in capped_dict.items():
        if name not in organized_by_name.keys():
            organized_by_name[name] = [number]
        else:
            organized_by_name[name].append(number)

    for name, id_list in organized_by_name.items():
        for n_id in id_list[1:]:
            merge(connection, id_list[0], n_id, sub_path, add_path)
            if name not in merged.keys():
                merged[name] = [id_list[0], n_id]
            else:
                merged[name].append(n_id)

    with open(log_path, 'a') as log:
        log.write('Merged by name:\n')
        pprint(merged, stream=log)

def main(category, config_path):
    config = get_config(config_path)
    connection = Connection(config.get('namespace'), config.get('email'), config.get('password'), config.get('update_endpoint'), config.get('query_endpoint'))

    timestamp = datetime.datetime.now().strftime("%Y_%m_%d")
    path = make_folders('data_out', [timestamp,])
    sub_file = 'dupe_sub_out.rdf'
    add_file = 'dupe_add_in.rdf'
    log_file = 'dupe_logs.txt'
    sub_path = os.path.join(path, sub_file)
    add_path = os.path.join(path, add_file)
    log_path = os.path.join(path, log_file)

    if category == '-j':
        pre_dict = get_list(getattr(queries, 'get_journal_list'), connection)
        simplified_dict = sort_journals(pre_dict, connection, sub_path, add_path, log_path)

    merge_by_name(simplified_dict, connection, sub_path, add_path, log_path)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])