from django.test import TestCase
from django.core.management import call_command
from .models import Book
import seeker

class QueryTests (TestCase):
    fixtures = ('books',)

    def setUp(self):
        call_command('reindex')
        self.document = seeker.get_mappings(Book)[0]

    def test_query(self):
        results = self.document.search().query('query_string', query='Django').execute()
        self.assertEqual(set(int(r.id) for r in results), set([2]))
    """
    def test_filter(self):
        results = self.mapping.query(filters={'authors': 'Alexa Watson'})
        self.assertEqual(set(r.data['title'] for r in results), set(['Herding Cats', 'Law School Sucks']))
        results = self.mapping.query(filters=seeker.F(in_print=False))
        self.assertEqual(results.count(), 1)
        self.assertEqual(results[0].id, '3')

    def test_facets(self):
        facet = seeker.TermAggregate('category')
        results = self.mapping.query(facets=facet)
        self.assertEqual(results.aggregates[facet], [{'key': 'Non-Fiction', 'doc_count': 2}, {'key': 'Fiction', 'doc_count': 1}])

    def test_crossquery(self):
        results = {}
        total = 0
        for result in seeker.crossquery('django'):
            results.setdefault(result.type, []).append(result)
            total += 1
        self.assertEqual(total, 3)
        self.assertEqual(len(results['book']), 1)
        self.assertEqual(len(results['magazine']), 2)
    """
