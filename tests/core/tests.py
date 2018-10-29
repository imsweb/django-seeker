from django.test import TestCase
from django.core.management import call_command
from .models import Book
from .mappings import BookMapping
import seeker


class QueryTests (TestCase):
    fixtures = ('books',)

    def setUp(self):
        call_command('reindex', 'core', quiet=True)
        self.mapping = seeker.get_model_mappings(Book)[0]

    def test_query(self):
        results = self.mapping.query(query='Django')
        self.assertEqual(set(int(r.id) for r in results), set([2]))

    def test_filter(self):
        results = self.mapping.query(filters={'authors': 'Alexa Watson'})
        self.assertEqual(set(r.data['title'] for r in results), set(['Herding Cats', 'Law School Sucks']))

    def test_boolean_filter(self):
        results = self.mapping.query(filters=seeker.F(in_print=False))
        self.assertEqual(results.count(), 1)
        self.assertEqual(results[0].id, '3')

    def test_TermAggregate(self):
        facet = seeker.TermAggregate('category', size=15)
        self.assertEqual(
            facet.to_elastic(),
            {
                'terms': {
                    'field': 'category',
                    'size': 15,
                }
            }
        )

    def test_facets(self):
        facet = seeker.TermAggregate('category')
        results = self.mapping.query(facets=facet)
        self.assertEqual(
            results.aggregates[facet],
            [{'key': 'fiction', 'doc_count': 3}, {'key': 'non', 'doc_count': 2}]
        )

    def test_crossquery(self):
        results = {}
        total = 0
        for result in seeker.crossquery('django'):
            results.setdefault(result.type, []).append(result)
            total += 1
        self.assertEqual(total, 3)
        self.assertEqual(len(results['book']), 1)
        self.assertEqual(len(results['magazine']), 2)


class MappingTests(TestCase):

    def test_mapping_refresh(self):
        """Just make sure refresh doesn't throw an error"""
        mapping = BookMapping()
        mapping.refresh()
