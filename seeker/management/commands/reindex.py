from django.core.management.base import BaseCommand
from django.apps import apps
from seeker.registry import documents
from elasticsearch.helpers import bulk
from elasticsearch_dsl.connections import connections
from optparse import make_option
import tqdm
import sys
import gc

def reindex(doc_class, options):
    """
    Index all the things, using ElasticSearch's bulk API for speed.
    """
    def get_actions():
        for doc in doc_class.documents():
            action = {
                '_index': doc_class._doc_type.index,
                '_type': doc_class._doc_type.name,
            }
            action.update(doc)
            yield action
    es = connections.get_connection(doc_class._doc_type.using)
    actions = get_actions() if options['quiet'] else tqdm.tqdm(get_actions(), total=doc_class.count(), leave=True)
    bulk(es, actions)
    es.indices.refresh(index=doc_class._doc_type.index)
    if not options['quiet']:
        print()

class Command (BaseCommand):
    args = '<app1 app2 ...>'
    help = 'Re-indexes the specified applications'
    option_list = BaseCommand.option_list + (
        make_option('--quiet',
            action='store_true',
            dest='quiet',
            default=False,
            help='Do not produce any output while indexing'),
        )

    def handle(self, *args, **options):
        for doc_class in documents:
            if not options['quiet']:
                print('Indexing %s' % doc_class)
            doc_class.clear()
            doc_class.init()
            reindex(doc_class, options)
            gc.collect()
