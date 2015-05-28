from django.core.management.base import BaseCommand
from django.apps import apps
from seeker.registry import documents, app_documents
from seeker.utils import progress
from elasticsearch.helpers import bulk
from elasticsearch_dsl.connections import connections
from optparse import make_option
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
    actions = get_actions() if options['quiet'] else progress(get_actions(), count=doc_class.count(), label=doc_class.__name__)
    bulk(es, actions)
    es.indices.refresh(index=doc_class._doc_type.index)

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
        doc_classes = []
        for label in args:
            doc_classes.extend(app_documents.get(label, []))
        if not args:
            doc_classes.extend(documents)
        for doc_class in doc_classes:
            doc_class.clear()
            doc_class.init()
            reindex(doc_class, options)
            gc.collect()
