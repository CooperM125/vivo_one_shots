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
import oneshots

def main():

    (opts, args) = parse_args()
    configure_logging(opts.quiet)
    config = get_config(opts.config)

    logging.info("Starting MultiShot...")
    one_shot_iterator(config, logging, opts.queryPath, opts.oneShotPath, opts.quiet)
    logging.info("Finished MultiShot... data_out file has triples to upload")
    return

def parse_args(args=sys.argv) -> (list, list):
    parser = optparse.OptionParser()
    parser.add_option("--fixUri", action="store", dest="uri", default="", help="Not yet implamented")  # TODO not yet implamented
    parser.add_option("--queries", action="store", dest="queryPath", default="queries", help="")
    parser.add_option("--oneShots", action="store", dest="oneShotPath", default="oneshots", help="")
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
    all_queries = [f for f in os.listdir(queries_path) if f.endswith('.rq')]
    all_queries.sort()

    all_oneshots = [f for f in os.listdir(oneShot_path) if f.endswith('.py') and f != '__init__.py']
    all_oneshots.sort()
    pairs = match_oneshot_to_query(all_oneshots, all_queries, logging)
    logging.info("Queries that will be run: %s", all_queries)

    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))
    logging.info('Running queries...')

    subjects = []
    total_count = 0
    # add counter for number of data problems fixed
    for query_file in all_queries:  # gets all subjects that need to be changed.
        logging.info("Opened query file %s", query_file)
        cleaner_name = pairs[query_file][:-3]
        logging.info('Attempting %s query...', query_file[:-3])
        subjects = query_uri(aide, queries_path + '/' + query_file, quiet)
        logging.info('Query was successful, %s dataproblems found in %s query', len(subjects), query_file[:-3])

        # get trips for both add and sub (try and accept)
        # # make function for add and sub (may need to run both at once instead of one at a time)
        corrected = False
        try:
            count_sub = sub_cleaner(aide, subjects, cleaner_name, quiet)
            corrected = True
        except AttributeError:
            logging.warn('Could not run sub_cleaner')
            count_sub = 0
            pass
        try:
            count_add = add_cleaner(aide, subjects, cleaner_name, quiet)
            corrected = True
        except AttributeError:
            if not corrected:
                logging.error('Failed to run oneshot %s', query_file[:-3])
                sys.exit(2)
            else:
                logging.warn('Could not run add_cleaner')
                count_add = 0
            pass
        total_count += (count_add + count_sub)
        logging.info('Datum corrected %s using %s', count_add + count_sub, cleaner_name)
    logging.info('Total datum corrected %s', total_count)


def add_cleaner(aide, subjects, cleaner_name, quiet):
    count = 0
    oneShot = getattr(oneshots, cleaner_name)  # NOTE must be a better way
    oneShot_func = getattr(oneShot, 'get_add_trips')  # returns the gettrips function for specific cleaner.

    for subject in subjects:
        count += 1
        try:
            trips = oneShot_func(aide, subject, quiet)
            save_trips(aide, subject, trips, cleaner_name + '_add')  # saves to file data_out
        except AttributeError:
            logging.error('Could not run oneshot %s on subject %s', cleaner_name, subject)
            break
    return count

def sub_cleaner(aide, subjects, cleaner_name, quiet):
    count = 0
    oneShot = getattr(oneshots, cleaner_name)  # NOTE must be a better way
    oneShot_func = getattr(oneShot, 'get_sub_trips')

    for subject in subjects:
        count += 1
        try:
            trips = oneShot_func(aide, subject, quiet)
            save_trips(aide, subject, trips, cleaner_name + '_sub')  # saves to file data_out
        except AttributeError:
            logging.error('Could not run oneshot %s on subject %s', cleaner_name, subject)
            break
    return count


def match_oneshot_to_query(oneshots, queries, logging):
    '''
    matches oneshot file name to query and return dictionary of query and oneshots
    '''
    pairs = {}
    for query_file in queries:
        if str('clean_' + query_file[:-2] + 'py') in oneshots:  # UGLY but works
            pairs[query_file] = str('clean_' + query_file[:-2] + 'py')
        else:
            logging.error('Could not find matching oneshot for query %s', query_file)
    logging.debug('All Queries matched with OneShots')
    return pairs

def save_trips(aide, subject, triples, cleaner_name):
    '''
    saves trips to be uploaded in data_out directory
    '''
    timestamp = datetime.now().strftime("%Y_%m_%d")
    path = 'data_out/' + timestamp + '/' + subject.split('/')[-1]
    try:
        os.makedirs(path)
    except FileExistsError:
        pass
    sub_file = os.path.join(path, cleaner_name + '.rdf')
    aide.create_file(sub_file, triples)

def query_uri(aide, file, quiet) -> (list):
    uris = []
    f = open(file, 'r')
    query = f.read()
    res = aide.do_query(query, quiet)
    uri_type = fetch_uri_type(res)
    for listing in res['results']['bindings']:
        uris.append(aide.parse_json(listing, uri_type))  # needs to be generlized (pubs)
    return uris

def fetch_uri_type(res):
    type = list(res['results']['bindings'][0])[0]
    return type

if __name__ == '__main__':
    main()
