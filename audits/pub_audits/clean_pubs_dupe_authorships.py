'''
Use on publications that have authors related multiple times.
This tool is for when a single instance of an author is listed multiple times.
Produces a file to sub out triples.
'''

from ..AuditABC import AuditABC
from utils import Aide


class CleanPubsDupeAuthorships(AuditABC):

    def __init__(self, aide: Aide):
        super.__init__(aide)
        self.subject_types = ['Document', 'AcademicArticle']

    def get_sub_trips(self, subject):
        '''
        gets trips that should be removed from vivo with this specific data problem
        '''
        q = '''\
        SELECT ?author ?relation
        WHERE {{
        <{}> <http://vivoweb.org/ontology/core#relatedBy> ?relation .
        ?relation <http://vivoweb.org/ontology/core#relates> ?author .
        ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
        }}
        '''.format(subject)

        res = self.aide.do_query(q)
        authors = {}
        triples = []
        for listing in res['results']['bindings']:
            uri = self.aide.parse_json(listing, 'author')
            relation = self.aide.parse_json(listing, 'relation')
            if uri not in authors.keys():
                authors[uri] = relation
            else:
                triples.extend(self.aide.get_all_triples(relation))
        return triples

    def get_add_trips(self, subject):
        return []
