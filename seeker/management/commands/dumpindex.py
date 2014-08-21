from django.core.management.base import BaseCommand
from django.apps import apps
from seeker.utils import get_app_mappings
from elasticsearch.helpers import scan
from optparse import make_option
import json
import sys

class Command (BaseCommand):
    args = '<app1 app2 ...>'
    help = 'Re-indexes the specified applications'
    option_list = BaseCommand.option_list + (
        make_option('--indent',
            type='int',
            dest='indent',
            default=None,
            help='Amount of indentation to use when serializing documents'
        ),
    )

    def handle(self, *args, **options):
        app_labels = args or [a.label for a in apps.get_app_configs()]
        output = sys.stdout
        output.write('[\n')
        for app_label in app_labels:
            for mapping in get_app_mappings(app_label):
                for idx, doc in enumerate(scan(mapping.es, index=mapping.index_name, doc_type=mapping.doc_type)):
                    if idx > 0:
                        output.write(',\n')
                    output.write(json.dumps(doc, indent=options['indent']))
        output.write('\n]\n')
