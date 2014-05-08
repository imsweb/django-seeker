from django.test import TestCase
from django.core.management import call_command
from .models import Book
import seeker

class QueryTests (TestCase):
    fixtures = ('books',)

    def setUp(self):
        call_command('reindex', 'core', quiet=True)
        self.mapping = seeker.get_model_mappings(Book)[0]

    def test_query(self):
        results = self.mapping.query(query='Django')
        self.assertEqual(set(int(r.id) for r in results), set([2]))
