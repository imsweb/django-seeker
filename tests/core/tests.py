from django.core.management import call_command
from django.test import TestCase

import seeker

from .external import BaseDocument
from .mappings import BookDocument, DerivedDocument, DjangoBookDocument
from .models import Book, Category


class QueryTests (TestCase):
    fixtures = ('books',)

    def setUp(self):
        call_command('reindex', quiet=True, drop=True)

    def test_registry(self):
        book_docs = set(seeker.model_documents[Book])
        self.assertEqual(book_docs, set([BookDocument, DjangoBookDocument]))
        # Make sure documents defined outside "mappings" modules are ignored (by default).
        self.assertIn(DerivedDocument, seeker.documents)
        self.assertNotIn(BaseDocument, seeker.documents)
        # All the documents (so far) in these tests are in the "core" app.
        self.assertEqual(set(seeker.app_documents['core']), set(seeker.documents))

    def test_query(self):
        results = BookDocument.search().query('query_string', query='django').execute()
        self.assertEqual(set(int(r.meta.id) for r in results), set([2]))
        results = BookDocument.search().query('term', title='herd').execute()
        self.assertEqual(set(int(r.meta.id) for r in results), set([1]))
        self.assertIsInstance(results[0], BookDocument)
        # Test multi-model seeker.search
        results = seeker.search(models=(Book,)).query('term', title='herd').execute()
        self.assertEqual(set(int(r.meta.id) for r in results), set([1]))
        self.assertIsInstance(results[0], BookDocument)

    def test_filter(self):
        results = BookDocument.search().filter('term', **{'authors.raw': 'Alexa Watson'}).execute()
        self.assertEqual(set(r.title for r in results), set(['Herding Cats', 'Law School Sucks']))
        results = BookDocument.search().filter('term', in_print=False).execute()
        self.assertEqual(results.hits.total, 1)
        self.assertEqual(results[0].meta.id, '3')

    def test_facets(self):
        facet = seeker.TermsFacet('category.raw')
        search = facet.apply(BookDocument.search())
        data = facet.data(search.execute())
        self.assertIn('buckets', data)
        self.assertEqual(data['buckets'], [{'key': 'Non-Fiction', 'doc_count': 2}, {'key': 'Fiction', 'doc_count': 1}])

    def test_filtered_queryset(self):
        self.assertEqual(DjangoBookDocument.search().count(), 1)
        all_books = set(r.meta.id for r in BookDocument.search().execute())
        django_books = set(r.meta.id for r in DjangoBookDocument.search().execute())
        self.assertTrue(django_books.issubset(all_books))

    def test_index_delete(self):
        # Make sure new books are only indexed into the documents that include them in their querysets.
        all_books = BookDocument.search().count()
        django_books = DjangoBookDocument.search().count()
        new_books = [
            Book.objects.create(title='I Love Django'),
            Book.objects.create(title='I Love Python'),
        ]
        for book in new_books:
            seeker.index(book)
        self.assertEqual(BookDocument.search().count(), all_books + 2)
        self.assertEqual(DjangoBookDocument.search().count(), django_books + 1)
        for book in new_books:
            seeker.delete(book)
        self.assertEqual(BookDocument.search().count(), all_books)
        self.assertEqual(DjangoBookDocument.search().count(), django_books)
