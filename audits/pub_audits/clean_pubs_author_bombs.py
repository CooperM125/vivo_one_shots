'''
This tool cleans out articles with hundreds or even thousands of authors that have clearly been mistakenly attributed.
If the author has no other publications besides the target article, the author's profile is deleted.
'''

from ..AuditABC import AuditABC
from utils import Aide


class Person:
    def __init__(self):
        self.real = True
        self.authorships = []


class CleanPubsAuthorBombs(AuditABC):

    def __init__(self, aide: Aide):
        super.__init__(aide)
        self.subject_types = ['Person']

    def get_authors(self, subject):
        # TODO: Make more nuanced check of items to know if author is a ghost
        q = '''\
        SELECT ?author (count(?article) as ?articles)
        WHERE {{
        <{}> <http://vivoweb.org/ontology/core#relatedBy> ?relation .
        ?relation <http://vivoweb.org/ontology/core#relates> ?author .
        ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
        ?author <http://vivoweb.org/ontology/core#relatedBy> ?authorship .
        ?authorship <http://vivoweb.org/ontology/core#relates> ?article .
        ?article <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/ontology/bibo/Article> .
        }}
        GROUP BY ?author
        '''.format(subject)

        res = self.aide.do_query(q)
        authors = {}
        for listing in res['results']['bindings']:
            author = Person()
            uri = self.aide.parse_json(listing, 'author')
            count = self.aide.parse_json(listing, 'articles')
            if count == 1:
                author.real = False

            authors[uri] = author

        return authors

    def get_relates(self, authors, subject):
        q = '''\
        SELECT ?author ?relation
        WHERE {{
        <{}> <http://vivoweb.org/ontology/core#relatedBy> ?relation .
        ?relation <http://vivoweb.org/ontology/core#relates> ?author .
        ?author <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
        }}
        '''.format(subject)

        res = self.aide.do_query(q)

        for listing in res['results']['bindings']:
            auth_uri = self.aide.parse_json(listing, 'author')
            relation = self.aide.parse_json(listing, 'relation')
            authors[auth_uri].authorships.append(relation)

        return authors

    def get_triples(self, authors):
        triples = []
        for uri, person in authors.items():
            for ship in person.authorships:
                triples.extend(self.aide.get_all_triples(ship))
            if not person.real:
                triples.extend(self.aide.get_all_triples(uri))
        return triples

    def get_sub_trips(self, subject):
        '''
        gets trips that should be removed from vivo for multishot
        '''
        # TODO Test to see if subject applies here
        authors = self.get_authors(self.aide, subject)
        authors = self.get_relates(self.aide, authors, subject)
        triples = self.get_triples(self.aide, authors)
        return triples

    def get_add_trips(self, subject):
        return []
