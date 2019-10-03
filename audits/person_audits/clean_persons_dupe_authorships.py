'''
Use on a Person profile that has duplicate authorships with publications.
This tool is for when a single instance of an article is listed multiple times.
Produces a file to sub out triples
'''

from utils import Aide
from audits.AuditABC import AuditABC


class CleanPersonsDuplicateAuthorships(AuditABC):

    def __init__(self, aide: Aide):
        super.__init__(aide)
        self.subject_types = ['Person']

    def get_add_trips(self, subject):
        return []

    def get_sub_trips(self, subject):
        query = '''\
        SELECT ?authorship ?article
        WHERE
        {{
            <{}> <http://vivoweb.org/ontology/core#relatedBy> ?authorship .
            ?authorship <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vivoweb.org/ontology/core#Authorship> .
            ?authorship <http://vivoweb.org/ontology/core#relates> ?article .
            ?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
        }}
        '''.format(subject)

        res = self.aide.do_query(query)
        articles = {}
        triples = []
        for listing in res['results']['bindings']:
            authorship = self.aide.parse_json(listing, 'authorship')
            article = self.aide.parse_json(listing, 'article')

            if article not in articles.keys():
                articles[article] = authorship
            else:
                triples = self.aide.get_all_triples(authorship)
        return triples
