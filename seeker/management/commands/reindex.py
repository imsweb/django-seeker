from django.core.management.base import BaseCommand
from django.apps import apps
from seeker.registry import model_documents
from elasticsearch.helpers import bulk
from elasticsearch_dsl.connections import connections
import sys
import gc

def reindex(doc_class, options):
    """
    Index all the things, using ElasticSearch's bulk API for speed.
    """
    action = {
        '_index': doc_class._doc_type.index,
        '_type': doc_class._doc_type.name,
    }
    total = {
        'count': 0,
    }
    def get_actions():
        for obj in doc_class.get_objects():
            action.update({
                '_id': doc_class.get_id(obj),
                '_source': doc_class.get_data(obj),
            })
            yield action
            total['count'] += 1
    es = connections.get_connection(doc_class._doc_type.using)
    bulk(es, get_actions())
    es.indices.refresh(index=doc_class._doc_type.index)
    print(total['count'])

class Command (BaseCommand):
    args = '<app1 app2 ...>'
    help = 'Re-indexes the specified applications'

    def handle(self, *args, **options):
        for model_class, doc_class in model_documents.items():
            print('Indexing %s (%s)... ' % (doc_class.__name__, model_class.__name__), end='')
            doc_class.clear()
            doc_class.init()
            reindex(doc_class, options)
            gc.collect()
