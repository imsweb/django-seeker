import io
import sys
import gc
import six

from django.core.management.base import BaseCommand
from django.apps import apps

from elasticsearch.helpers import bulk

from seeker.utils import get_app_mappings


def silent_iter(iterable, **kwargs):
    for obj in iterable:
        yield obj


try:
    from tqdm import tqdm as progress_iter
except BaseException:
    progress_iter = silent_iter


def reindex(mapping, options):
    """
    Index all the things, using ElasticSearch's bulk API for speed.
    """
    action = {
        '_index': mapping.index_name,
        '_type': mapping.doc_type,
    }

    def get_actions():
        for obj in mapping.get_objects(cursor=options['cursor']):
            action.update({
                '_id': mapping.get_id(obj),
                '_source': mapping.get_data(obj),
            })
            yield action
    try:
        total = mapping.queryset().count()
    except BaseException:
        total = None

    if six.PY2:
        writer = io.BytesIO
    elif six.PY3:
        writer = io.StringIO
    output = writer() if options['quiet'] else sys.stderr
    iterator = silent_iter if options['quiet'] else progress_iter
    output.write('Indexing %s\n' % mapping.__class__.__name__)
    output.flush()
    bulk(mapping.es, iterator(get_actions(), total=total, leave=True))
    output.write('\n')
    output.flush()


class Command (BaseCommand):
    help = 'Re-indexes the specified applications'

    def add_arguments(self, parser):
        parser.add_argument('app_labels',
                            nargs='*',
                            default=[],
                            help='Optional (space delimited) list of apps: <app1 app2 ...>'
                            )
        parser.add_argument('--quiet',
                            action='store_true',
                            dest='quiet',
                            default=False,
                            help='Suppress all output to stdout'
                            )
        parser.add_argument('--cursor',
                            action='store_true',
                            dest='cursor',
                            default=False,
                            help='Use a server-side cursor when fetching objects'
                            )
        parser.add_argument('--no-data',
                            action='store_false',
                            dest='data',
                            default=True,
                            help='Only reindex the mappings, not any data'
                            )
        parser.add_argument('--drop',
                            action='store_true',
                            dest='drop',
                            default=False,
                            help='Drops the index before re-indexing'
                            )

    def handle(self, *args, **options):
        dropped = set()
        app_labels = options['app_labels'] or [a.label for a in apps.get_app_configs()]
        for app_label in app_labels:
            for mapping in get_app_mappings(app_label):
                if options['drop'] and mapping.index_name not in dropped:
                    # Drop the index before re-indexing
                    if mapping.es.indices.exists(index=mapping.index_name):
                        mapping.es.indices.delete(index=mapping.index_name)
                        dropped.add(mapping.index_name)

                # If the index doesn't exist (or has been dropped), (re-)create it.
                if not mapping.es.indices.exists(index=mapping.index_name):
                    mapping.es.indices.create(index=mapping.index_name)

                # If the mapping (type) already exists, delete it first.
                mapping.clear()

                # Create the new mapping.
                mapping.refresh()

                if options['data']:
                    # Reindex everything.
                    reindex(mapping, options)
                    # Refresh the index, so documents are available for searching when this command completes.
                    mapping.es.indices.refresh(index=mapping.index_name)
                    # Clean up memory after potentially large indexing sets.
                    gc.collect()
