from .models import Book
from django.core.management import call_command
from django.test import TestCase
import seeker

from .external import BaseDocument
from .mappings import BookDocument, DerivedDocument
from .models import Book, Category

class QueryTests (TestCase):
    fixtures = ('books',)

    def setUp(self):
        call_command('reindex', quiet=True)
        self.document = seeker.model_documents[Book][0]

    def test_registry(self):
        # Make sure documents defined outside "mappings" modules are ignored (by default).
        self.assertIn(DerivedDocument, seeker.documents)
        self.assertNotIn(BaseDocument, seeker.documents)
        # All the documents (so far) in these tests are in the "core" app.
        self.assertEqual(set(seeker.app_documents['core']), set(seeker.documents))

    def test_query(self):
        results = self.document.search().query('query_string', query='django').execute()
        self.assertEqual(set(int(r.meta.id) for r in results), set([2]))
        results = self.document.search().query('term', title='herd').execute()
        self.assertEqual(set(int(r.meta.id) for r in results), set([1]))
        self.assertIsInstance(results[0], BookDocument)
        # Test multi-model seeker.search
        results = seeker.search(models=(Book,)).query('term', title='herd').execute()
        self.assertEqual(set(int(r.meta.id) for r in results), set([1]))
        self.assertIsInstance(results[0], BookDocument)

    def test_filter(self):
        results = self.document.search().filter('term', **{'authors.raw': 'Alexa Watson'}).execute()
        self.assertEqual(set(r.title for r in results), set(['Herding Cats', 'Law School Sucks']))
        results = self.document.search().filter('term', in_print=False).execute()
        self.assertEqual(results.hits.total, 1)
        self.assertEqual(results[0].meta.id, '3')

    def test_facets(self):
        facet = seeker.TermsFacet('category.raw')
        search = facet.apply(self.document.search())
        data = facet.data(search.execute())
        self.assertIn('buckets', data)
        self.assertEqual(data['buckets'], [{'key': 'Non-Fiction', 'doc_count': 2}, {'key': 'Fiction', 'doc_count': 1}])
