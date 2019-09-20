from datetime import datetime
import os
import logging
import yaml
import sys
import argparse
sys.path.append('.')

from utils import Aide


def main():

    (opts, args) = parse_args()
    configure_logging() # TODO: pass in quite. 
    config = get_config(opts.config_path)
    endpoint =  config.endpoint

    logging.info("Starting dfixer...")
    query_iterator(endpoint , opts.queryPath)
    # TODO Feed path of queries and have it pull triples that had data problems. 
    # TODO save triples to folder that have data problems 
    # TODO have fix for each data problem type
    # TODO file for uri that can not be automated / run into problems with checks. 

# TODO fix parse args 
def parse_args(args=sys.argv) -> (list, list):
    parser = argparse.ArgumentParser
    parser.add_argument("--quiet",
                      action="store_true",type="b" dest="quiet", default=False)
    parser.add_argument("--queries", action="store", type="string", dest="queryPath", default ="/queries") # maynot need this just call on the oneshots
    parser.add_argument("--oneShot", action="store", type="string", dest="oneShot", default="") 
    parser.add_argument("--config", action="store", type="string", dest="config", default="config.yaml")
    return parser.parse_args(args)
                      

def configure_logging():
    fmt = '%(asctime)s  %(levelname)-9s  %(message)s'
    logfile = 'dfixer.log'

    logging.basicConfig(level=logging.DEBUG, format=fmt, filename=logfile)

    if config.quiet:
        # Do not write logs to console; normal output still goes to stdout.
        return

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

def query_iterator(endpoint, path):
    """Loops over a list of SPARQL queries and sends them to an endpoint.

    Keyword arguments:
    endpoint -- an accessible SPARQL endpoint
    """
    report = ""
    all_queries = os.listdir(path)
    all_queries.sort()
    print ("Queries that will be run:")
    print (all_queries)
    print ("\n")
    aide = Aide(config.get('query_endpoint'), config.get('email'), config.get('password'))
    
    for query_file in all_queries:
        with open(path + query_file, 'r') as query_file_path:
            logging.info("Opened query file %s", query_file)
            result = aide.get_all_triples(query_file.read())
            uri = aide.parse_json(listing, 'author')
            #TODO save triple in file
            

    return somtheing # 


if __name__ == '__main__':
    main()