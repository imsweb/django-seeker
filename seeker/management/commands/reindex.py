import argparse

from django.conf import settings
from django.core.management.base import BaseCommand
from elasticsearch.helpers import bulk
from elasticsearch_dsl.connections import connections

from seeker.registry import app_documents, documents
from seeker.utils import progress, update_timestamp_index


def reindex(es, doc_class, index, options):
    """
    Index all the things, using ElasticSearch's bulk API for speed.
    """

    def get_actions():
        for doc in doc_class.documents(cursor=options['cursor']):
            action = {
                '_index': index,
            }
            action.update(doc)
            yield action

    actions = get_actions() if options['quiet'] else progress(get_actions(), count=doc_class.count(), label=doc_class.__name__)
    bulk(es, actions)
    es.indices.refresh(index=index)


class Command(BaseCommand):
    args = '<app1 app2 ...>'
    help = 'Re-indexes the specified applications'

    def add_arguments(self, parser):
        parser.add_argument('--using',
                            dest='using',
                            default=None,
                            help='The ES connection alias to use',
        )
        parser.add_argument('--index',
                            dest='index',
                            default=None,
                            help='The ES index to store data in',
        )
        parser.add_argument('--quiet',
                            action='store_true',
                            dest='quiet',
                            default=False,
                            help='Do not produce any output while indexing',
        )
        parser.add_argument('--drop',
                            action='store_true',
                            dest='drop',
                            default=False,
                            help='Drops the index before re-indexing',
        )
        parser.add_argument('--clear',
                            action='store_true',
                            dest='clear',
                            default=False,
                            help='Deletes all documents before re-indexing',
        )
        parser.add_argument('--no-data',
                            action='store_false',
                            dest='data',
                            default=True,
                            help='Only create the mappings, do not index any data',
        )
        parser.add_argument('--cursor',
                            action='store_true',
                            dest='cursor',
                            default=False,
                            help='Use a server-side cursor when fetching data for indexing',
        )
        parser.add_argument('args', nargs=argparse.REMAINDER)

    def handle(self, *args, **options):
        doc_classes = []
        for label in args:
            if '.' in label:
                app_name, doc_name = label.split('.', 1)
                doc_name = doc_name.lower()
                for doc_class in app_documents.get(app_name, []):
                    if doc_name == doc_class.__name__.lower():
                        doc_classes.append(doc_class)
                        break
            else:
                doc_classes.extend(app_documents.get(label, []))
        if not args:
            doc_classes.extend(documents)
        deleted_indexes = []
        for doc_class in doc_classes:
            using = options['using'] or doc_class._index._using or 'default'
            index = doc_class._index._name
            es = connections.get_connection(using)

            update_timestamp_index(index)

            if options['drop'] and index not in deleted_indexes:
                if es.indices.exists(index=index):
                    es.indices.delete(index=index)
                    deleted_indexes.append(index)
            elif options['clear']:
                doc_class.clear(index=index, using=using)
            doc_class.init(index=index, using=using)
            if options['data']:
                reindex(es, doc_class, index, options)
