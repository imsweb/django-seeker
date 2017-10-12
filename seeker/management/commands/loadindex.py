from django.core.management.base import BaseCommand, CommandError
from django.apps import apps
from seeker.utils import get_app_mappings
from elasticsearch.helpers import bulk
import json

class Command (BaseCommand):
    help = 'Loads data for the specified applications from a JSON dump file'

    def add_arguments(self, parser):
        parser.add_argument('app_labels',
            nargs='*',
            default=[],
            help='Optional (space delimited) list of apps: <app1 app2 ...>'
        )
        parser.add_argument('--filename', '-f',
            dest='filename',
            default=None,
            help='The file to load index data from'
        )

    def handle(self, *args, **options):
        if not options['filename']:
            raise CommandError('Please specify a file (-f) to read data from')

        app_labels = options['app_labels'] or [a.label for a in apps.get_app_configs()]
        doc_types = {}
        client = None
        refresh_indices = set()

        # TODO: support mappings located on separate servers
        for app_label in app_labels:
            for mapping in get_app_mappings(app_label):
                doc_types[mapping.doc_type] = mapping
                if client is None:
                    client = mapping.es

        def get_actions():
            for data in json.load(open(options['filename'], 'rb')):
                mapping = doc_types.get(data['_type'])
                if mapping:
                    refresh_indices.add(mapping.index_name)
                    data['_index'] = mapping.index_name
                    yield data

        bulk(client, get_actions())

        # Refresh the seen indices, so documents are available for searching when this command completes.
        for name in refresh_indices:
            client.indices.refresh(index=name)
