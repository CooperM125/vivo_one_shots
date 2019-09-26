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

    logging.info("Starting dfixer...")
    one_shot_iterator(config, logging, opts.queryPath, opts.oneShotPath)
    logging.info("Finished dfixer... data_out file has triples to upload")
    return

def parse_args(args=sys.argv) -> (list, list):
    parser = optparse.OptionParser()
    parser.add_option("--quiet", action="store_true", dest="quiet", default=False)
    parser.add_option("--queries", action="store", dest="queryPath", default="queries")  # maynot need this just call on the oneshots
    parser.add_option("--oneShots", action="store", dest="oneShotPath", default="oneshots")
    parser.add_option("--config", action="store", dest="config", default="config.yaml")
    parser.add_option("--fixUri", action="store", dest="uri", default="")  # TODO not yet implamented
    return parser.parse_args(args)


def configure_logging(be_quiet):
    fmt = '%(asctime)s  %(levelname)-9s  %(message)s'
    logfile = 'dfixer.log'

    logging.basicConfig(level=logging.DEBUG, format=fmt, filename=logfile)

    if be_quiet:
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

def one_shot_iterator(config, logging, queries_path, oneShot_path):
    """Loops over a list of one_shot fixes.
        AKA the multi_shot :)
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
    for query_file in all_queries:  # gets all subjects that need to be changed.
        logging.info("Opened query file %s", query_file)
        cleaner_name = pairs[query_file][:-3]
        # NOTE temp not in use for testing

        subjects = query_uri(aide, queries_path + '/' + query_file)

        # subjects.append('http://vivo.ufl.edu/individual/n6267060498')  # NOTE temp code
        # get attribute for specific data fix
        for subject in subjects:
            try:
                cleaner = getattr(oneshots, cleaner_name)  # NOTE must be a better way
                function = getattr(cleaner, 'get_trips')
                trips = function(aide, subject)
                save_trips(aide, subject, trips, cleaner_name)
            except AttributeError:
                logging.error('Could not run oneshot %s', cleaner_name)
                break
        # when done with all subjects
    # return something or write to file

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
    sub_file = os.path.join(path, cleaner_name +'.rdf')
    aide.create_file(sub_file, triples)

def query_uri(aide, file) -> (list):
    uris = []
    f = open(file, 'r')
    query = f.read()
    response = aide.do_query(query)

    # 2.) do query
    # 3.) parse and return uri list

    return uris

if __name__ == '__main__':
    main()
