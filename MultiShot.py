from datetime import datetime
import os
import logging
import yaml
import sys
import argparse
import optparse
import importlib
sys.path.append('.')

from utils import Aide
import audits
from audits.pub_audits import clean_pubs_dupe_authorships  # put in init.py
from audits.pub_audits import clean_pubs_author_bombs  # put in init.py
from audits import pub_audits
from audits import person_audits
from audits import misc_audits


def main():

    (opts, args) = parse_args()
    configure_logging(opts.quiet)
    config = get_config(opts.config)

    logging.info("Starting MultiShot...")
    # other modes
    if opts.uri != "":
        single_target(config, logging, opts.uri, opts.oneShotPath, opts.quiet)
    else:
        one_shot_iterator(config, logging, opts.queryPath, opts.oneShotPath, opts.quiet)
    logging.info("Finished MultiShot... data_out file has triples to upload")
    return

def parse_args(args=sys.argv) -> (list, list):
    parser = optparse.OptionParser()
    parser.add_option("--fixUri", action="store", dest="uri", default="", help="")  
    parser.add_option("--queries", action="store", dest="queryPath", default="queries", help="")
    parser.add_option("--oneShots", action="store", dest="oneShotPath", default="audits", help="")
    parser.add_option("--quiet", action="store_true", dest="quiet", default=False, help="Set this flagg to quiet terminal output")
    parser.add_option("--config", action="store", dest="config", default="config.yaml", help="")
    return parser.parse_args(args)


def configure_logging(quiet):
    fmt = '%(asctime)s  %(levelname)-9s  %(message)s'
    logfile = 'MultiShot.log'

    logging.basicConfig(level=logging.DEBUG, format=fmt, filename=logfile)

    if quiet:
        # Do not write logs to console; normal output still goes to stdout.
        return

    console = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(fmt)
    console.setFormatter(formatter)

    console.setLevel(logging.INFO)

    logging.getLogger().addHandler(console)
    return

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except Exception as e:
        print("Error: Check config file")
        exit(e)
    return config

def one_shot_iterator(config, logging, queries_path, oneShot_path, quiet):
    """Loops over a list of one_shot fixes.
        aka the heart of MultiShot :)
    """

    all_oneshots, all_queries = load_oneshots(queries_path, oneShot_path, logging)
    pairs = match_oneshot_to_query(all_oneshots, all_queries, logging)

    logging.info("Queries that will be run: %s", all_queries)

    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'), quiet)
    logging.info('Running queries...')
    subjects = []
    total_count = 0
    # add counter for number of data problems fixed
    for query_file in all_queries:  # gets all subjects that need to be changed.
        logging.info("Opened query file %s", query_file)
        cleaner_name = pairs[query_file][:-3]
        logging.info('Attempting %s query...', query_file[:-3])
        subjects = query_uri(aide, queries_path + '/' + query_file)
        logging.info('Query was successful, %s dataproblems found in %s query', len(subjects), query_file[:-3])

        count = cleaners(aide, cleaner_name, subjects)

def single_target(config, logging, uri, oneShot_path, quiet):
    '''
    runs all multishot that apply to one uri if applicable.
    '''

    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'), quiet)
    type = fetch_uri_type(aide, uri)

    dir = 'audits/' + type + 'audits'
    listD = os.listdir(dir)
    # pull all oneshots
    oneshots = ([f[:-3] for f in os.listdir(dir) if f.endswith('.py') and f != '__init__.py'])
    oneshots.sort()
    # iterorate through theme all
    list_uri = []
    list_uri.append(uri)
    for oneshot in oneshots:
        count = cleaners(aide, oneshot, list_uri)


def match_oneshot_to_query(oneshots, queries, logging):
    '''
    matches oneshot file name to query and return dictionary of query and oneshots
    '''
    pairs = {}
    for query_file in queries:
        val = str('clean_' + query_file[:-2] + 'py')
        if val in oneshots:  # UGLY but works
            pairs[query_file] = str('clean_' + query_file[:-2] + 'py')
        else:
            logging.warn('Could not find matching oneshot for query %s', query_file)
    logging.debug('All Queries matched with OneShots')
    return pairs

def save_trips(aide, subject, triples, cleaner_name):
    '''
    saves trips to be uploaded in data_out directory
    '''
    if triples is not None:
        logging.debug('Saving tripples for %s', cleaner_name)
        timestamp = datetime.now().strftime("%Y_%m_%d")
        path = 'data_out/' + timestamp + '/' + subject.split('/')[-1]
        try:
            os.makedirs(path)
        except FileExistsError:
            pass
        sub_file = os.path.join(path, cleaner_name + '.rdf')
        aide.create_file(sub_file, triples)
    else:
        logging.debug('Trips %s has not triples to save', cleaner_name)

def query_uri(aide, file) -> (list):
    uris = []
    f = open(file, 'r')
    query = f.read()
    res = aide.do_query(query)
    uri_type = parse_uri_type(res)
    for listing in res['results']['bindings']:
        uris.append(aide.parse_json(listing, uri_type))
    return uris

def load_oneshots(queries_path, oneShot_path, logging):
    # defualt - go to every folder that ends with _audits and pull all .py files except
    try:
        if oneShot_path == 'audits':
            files = [f for f in os.listdir('audits') if f.endswith('_audits')]
            all_oneshots = []
            for file in files:
                all_oneshots += ([f for f in os.listdir('audits/' + file) if f.endswith('.py') and f != '__init__.py'])
            all_oneshots.sort()
        # input path -  go to input path and do same thing
        else:
            all_oneshots = [f for f in os.listdir(oneShot_path) if f.endswith('.py') and f != '__init__.py']
            all_oneshots.sort()

        all_queries = [f for f in os.listdir(queries_path) if f.endswith('.rq')]
        all_queries.sort()
    except Exception:
        logging.error('Failed to load all queries and oneshots')
        sys.exit(2)

    return all_oneshots, all_queries

def parse_uri_type(res):
    type = list(res['results']['bindings'][0])[0]
    return type

def check_cleaner_type(cleaner_name):
    '''
    check what type of cleaner given name
    '''
    if 'pubs_' in cleaner_name:
        type = 'pub_audits'
    elif 'person_' in cleaner_name:
        type = 'person_audits'
    elif 'misc_' in cleaner_name:
        type = 'misc_audits'
    else:
        type = 'misc_audits'
        logging.error('Could not find cleaner %s', cleaner_name)
    return type

def fetch_uri_type(aide, uri) -> str:
    '''
    queries the uri to find if its a pub, author, ext..
    currently only suppoprts person and accademicartical type
    '''
    q = '''\
    SELECT ?types
    WHERE {{
    <{}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?types .
    }}
        '''.format(uri)
    res = aide.do_query(q)
    for type in res['results']['bindings']:
        uri_type = type['types']['value']
        # TODO add test to see if multiple types at once
        if 'Person' in uri_type:
            return 'person_'
        elif 'AcademicArticle' in uri_type:
            return 'pub_'
    logging.error('Could not find type for %s', uri)
    sys.exit(2)

def cleaners(aide, oneshot_name, uris):
    '''
    Takes in cleaner name and list of uris and outputs the triples to be removed and added
    '''
    logging.info('Starting oneshot for %s', oneshot_name)
    # TODO Change when using ABC
    type = check_cleaner_type(oneshot_name)
    files = getattr(audits, type)
    oneShot = getattr(files, oneshot_name)

    # remove this bit of code when turning to abc
    hasCleaner = False
    try:
        add_function = getattr(oneShot, 'get_add_trips')
        hasCleaner = True
    except AttributeError:
        logging.warn('Cleaner %s does not have get_add_trips function', oneshot_name)
        add_function = None
    try:
        sub_function = getattr(oneShot, 'get_sub_trips')
        hasCleaner = True
    except AttributeError:
        logging.warn('Cleaner %s does not have get_add_trips function', oneshot_name)
        sub_function = None
    # remove this bit of code when turning to abc

    count = 0
    for uri in uris:
        if add_function != None:
            try:
                add_trips = add_function(aide, uri)
                count += len(add_trips)
            except Exception:
                logging.error('Cleaner %s failed, skipping uri %s', oneshot_name, uri)
        else:
            logging.warn('uri %s does not neede to run %s ', uri, oneshot_name)

        if sub_function != None:
            try:
                sub_trips = sub_function(aide, uri)
                count += len(sub_trips)
            except Exception:
                logging.error('Cleaner %s failed, skipping uri %s', oneshot_name, uri)
        else:
            logging.warn('uri %s does not neede to run %s ', uri, oneshot_name)

        if add_function != None:
            save_trips(aide, uri, add_trips, oneshot_name + '_add')
        if sub_function != None:
            save_trips(aide, uri, sub_trips, oneshot_name + '_sub')
    return count


if __name__ == '__main__':
    main()
