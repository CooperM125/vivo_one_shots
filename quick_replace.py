import requests
import sys

import yaml

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except:
        print("Error: Check config file")
        exit()
    return config

def do_update(query, config):
    print("Query:\n" + query)
    payload = {
        'email': config.get('email'),
        'password': config.get('password'),
        'update': query,
    }
    response = requests.post(config.get('update_endpoint'), params=payload, verify=False)
    print(response)
    return response

def main(config_path):
    config = get_config(config_path)
    bad_triple = ""
    good_triple = ""

    d_query = """
        DELETE DATA {{
            GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> {{
                {} .
            }}
        }}
        """.format(bad_triple)

    i_query="""
        INSERT DATA {{
            GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> {{
                {} .
            }}
        }}
        """.format(good_triple)

    do_update(d_query, config)
    do_update(i_query, config)

if __name__ == '__main__':
    main(sys.argv[1])