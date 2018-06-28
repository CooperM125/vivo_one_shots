#Fix bad strings using mysql database
import csv
import datetime
import os.path
import sys

def make_folders(top_folder, sub_folders=None):
    if not os.path.isdir(top_folder):
        os.mkdir(top_folder)

    if sub_folders:
        sub_top_folder = os.path.join(top_folder, sub_folders[0])
        top_folder = make_folders(sub_top_folder, sub_folders[1:])

    return top_folder

def main(infile):
    timestamp = datetime.datetime.now().strftime("%Y_%m_%d")
    full_path = make_folders('data_out', ['from_csv', timestamp,])
    sub_file = os.path.join(full_path, 'sub_out.rdf')
    add_file = os.path.join(full_path, 'add_in.rdf')

    tag = '^^<http://www.w3.org/2001/XMLSchema#string>'
    old_trips = []
    clean_trips = []
    with open(infile, 'r') as source:
        reader = csv.reader(source, delimiter=',', quotechar='|')
        # import pdb
        # pdb.set_trace()
        for row in reader:
            try:
                s_record, s, p_record, p, o_record, o = row
                o = o.replace('"', '\\"')
                o = o.replace('\n\\\n', '\\r\\n')
            except Exception as e:
                print("Error in following row:")
                print(row)
                print(e)
                exit()
            if p=='http://vivoweb.org/ontology/core#isCorrespondingAuthor':
                continue
            else:
                old_trips.append('<' + s + '> <' + p + '> "' + o + '" .')
                clean_trips.append('<' + s + '> <' + p + '> "' + o + '"' + tag + ' .')

    with open(sub_file, 'w') as sub:
        sub.write("\n".join(old_trips))

    with open(add_file, 'w') as add:
        add.write("\n".join(clean_trips))

if __name__ == '__main__':
    main(sys.argv[1])