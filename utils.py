import requests

class Aide(object):
    def __init__(self, query_endpoint, email, password):
        self.q_endpoint = query_endpoint
        self.email = email
        self.password = password

    def do_query(self, query, silent=False):
        if not silent:
            print("Query:\n" + query)
        payload = {
            'email': self.email,
            'password': self.password,
            'query': query,
        }
        headers = {'Accept': 'application/sparql-results+json'}
        response = requests.get(self.q_endpoint, params=payload, headers=headers, verify=False)
        if not silent:
            print(response)
        if response.status_code == 400:
            exit("Error: check query")
        elif response.status_code == 403:
            exit("Error: check credentials")
        elif response.status_code == 406:
            exit("Error: check accept header")
        if response.status_code == 500:
            exit("Error: check server")
        return response.json()

    def get_all_triples(self, subject, silent=False):
        triples = []
        # edit to specify graph k2 (right befoir where)
        s_query = """ SELECT ?s ?p ?o WHERE{{<{}> ?p ?o .}} """.format(subject)
        s_res = self.do_query(s_query, silent)
        for listing in s_res['results']['bindings']:
            pred = self.parse_json(listing, 'p', True)
            obj = self.parse_json(listing, 'o', True)
            trip = '<' + subject + '> ' + pred + ' ' + obj
            triples.append(trip)
        # edit to specify graph k2
        o_query = """ SELECT ?s ?p ?o WHERE{{?s ?p <{}> .}} """.format(subject)
        o_res = self.do_query(o_query, silent)
        for listing in o_res['results']['bindings']:
            subj = self.parse_json(listing, 's', True)
            pred = self.parse_json(listing, 'p', True)
            trip = subj + ' ' + pred + ' <' + subject + '> '
            triples.append(trip)

        return triples

    def parse_json(self, data, search, prep=False):
        try:
            value = data[search]['value']
        except KeyError:
            return None
            
        if prep: # link as literal. find data type of links (they may brake this)
            if 'http' in value:
                value = '<' + value + '>'
            else:
                datatype = data[search]['datatype']
                value = '"' + value + '"^^<' + datatype +'>'
        return value

    def create_file(self, filename, triples):
        with open(filename, 'a+') as rdf:
            rdf.write(" . \n".join(triples))
            rdf.write(" . \n")
