from django.core.management.base import BaseCommand
from django.apps import apps
from seeker.utils import get_app_mappings
from elasticsearch.helpers import scan
import json

class Command (BaseCommand):
    help = 'Dumps out data from the specified applications'

    def add_arguments(self, parser):
        parser.add_argument('app_labels',
            nargs='*',
            default=[],
            help='Optional (space delimited) list of apps: <app1 app2 ...>'
        )
        parser.add_argument('--indent',
            type='int',
            dest='indent',
            default=None,
            help='Amount of indentation to use when serializing documents'
        )

    def handle(self, *args, **options):
        app_labels = options['app_labels'] or [a.label for a in apps.get_app_configs()]
        output = self.stdout
        output.write('[')
        for app_label in app_labels:
            for mapping in get_app_mappings(app_label):
                for idx, doc in enumerate(scan(mapping.es, index=mapping.index_name, doc_type=mapping.doc_type)):
                    if idx > 0:
                        output.write(',')
                    output.write(json.dumps(doc, indent=options['indent']), ending='')
        output.write(']')
