from django.core.management.base import BaseCommand
from django.apps import apps
from seeker.utils import get_app_mappings
from elasticsearch.helpers import bulk
from optparse import make_option
import StringIO
import sys
import gc

def silent_iter(iterable, **kwargs):
    for obj in iterable:
        yield obj

try:
    from tqdm import tqdm as progress_iter
except:
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
    except:
        total = None
    output = StringIO.StringIO() if options['quiet'] else sys.stderr
    iterator = silent_iter if options['quiet'] else progress_iter
    output.write('Indexing %s\n' % mapping.__class__.__name__)
    output.flush()
    bulk(mapping.es, iterator(get_actions(), total=total, leave=True))
    output.write('\n')
    output.flush()

class Command (BaseCommand):
    args = '<app1 app2 ...>'
    help = 'Re-indexes the specified applications'
    option_list = BaseCommand.option_list + (
        make_option('--quiet',
            action='store_true',
            dest='quiet',
            default=False,
            help='Suppress all output to stdout'
        ),
        make_option('--cursor',
            action='store_true',
            dest='cursor',
            default=False,
            help='Use a server-side cursor when fetching objects'
        ),
        make_option('--no-data',
            action='store_false',
            dest='data',
            default=True,
            help='Only reindex the mappings, not any data'
        ),
    )

    def handle(self, *args, **options):
        app_labels = args or [a.label for a in apps.get_app_configs()]
        for app_label in app_labels:
            for mapping in get_app_mappings(app_label):
                # If the index doesn't exist, create it.
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
