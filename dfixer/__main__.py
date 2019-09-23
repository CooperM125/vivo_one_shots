from datetime import datetime
import os
import logging
import yaml
import sys
import argparse
import optparse
import importlib
from oneshots import*

sys.path.append('.')

from utils import Aide


def main():

    (opts, args) = parse_args()
    configure_logging() # TODO: pass in quite. 
    config = get_config(opts.config)

    logging.info("Starting dfixer...")
    one_shot_iterator(config , opts.queryPath, opts.oneShotPath)
    # TODO Feed path of queries and have it pull triples that had data problems. 
    # TODO save triples to folder that have data problems 
    # TODO have fix for each data problem type
    # TODO file for uri that can not be automated / run into problems with checks. 
    # TODO add option to fix one data problem

# TODO fix parse args 
def parse_args(args=sys.argv) -> (list, list):
    parser = optparse.OptionParser()
    parser.add_option("--quiet", action="store_true", dest="quiet", default=False)
    parser.add_option("--queries", action="store", dest="queryPath", default ="queries") # maynot need this just call on the oneshots
    parser.add_option("--oneShots", action="store", dest="oneShotPath", default="oneshots") 
    parser.add_option("--config", action="store", dest="config", default="config.yaml")
    return parser.parse_args(args)
                      

def configure_logging():
    fmt = '%(asctime)s  %(levelname)-9s  %(message)s'
    logfile = 'dfixer.log'

    logging.basicConfig(level=logging.DEBUG, format=fmt, filename=logfile)


    console = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter(fmt)
    console.setFormatter(formatter)

    console.setLevel(logging.INFO)

    logging.getLogger().addHandler(console)

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except Exception as e:
        print("Error: Check config file")
        exit(e)
    return config

def one_shot_iterator(config, queries_path, oneShot_path):
    """Loops over a list of one_shots and sends them to an endpoint.

    Keyword arguments:
    endpoint -- an accessible SPARQL endpoint
    """
    all_queries = os.listdir(queries_path)
    all_queries.sort()

    all_oneshots = os.listdir(oneShot_path) # change becuse of pycache
    all_oneshots.sort()
    pairs = match_oneshot_to_query(all_oneshots, all_queries) 
    print ("queries that will be run:")
    print (all_queries) # for loop for all elemetns in deictionaryt.
    print ("\n")
    # TODO: add to log file. (start)

    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))

    # NOTE: dchecker query may need to be modified to return just subjects(Not count)
    subjects =[]
    for query_file in all_queries: # gets all subjects that need to be changed. 
        with open(queries_path+'/' + query_file, 'r') as query_file_path:
            logging.info("Opened query file %s", query_file)
            cleaner = str('clean_'+ query_file[:-2] + 'py')
            #| NOTE temp not in use for testing
            #| result = aide.get_all_triples(query_file.read())
            #| uri = aide.parse_json(listing, 'author') # change to general subject? 
            subjects.append(query_file)# temp code
            # get attribute for specific data fix
            for subject in subjects:
                trips = getattr(cleaner,'get_trips')

            # get attribut 
    # return something

def match_oneshot_to_query(oneshots, queries):
    '''
    matches oneshot file name to query and return dictionary of query and oneshots
    '''
    pairs = {}
    for query_file in queries:
        if str('clean_'+ query_file[:-2]+'py') in oneshots: # ugly but works
            pairs[query_file] = str('clean_' + query_file[:-2]+'py')
        else:
            print ('Could not find matching oneshot for query %s',query_file)
            # TODO: add this to debug log
    
    return pairs


if __name__ == '__main__':
    main()