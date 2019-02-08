import json
from optparse import make_option

from django.apps import apps
from django.core.management.base import BaseCommand
from elasticsearch.helpers import scan
from elasticsearch_dsl.connections import connections


class Command(BaseCommand):
    args = '<doc_type>'
    help = 'Dumps out data from the specified document types'
    option_list = BaseCommand.option_list + (
        make_option('--indent',
                    type='int',
                    dest='indent',
                    default=None,
                    help='Amount of indentation to use when serializing documents',
        ),
        make_option('--index',
                    dest='index',
                    default=None,
                    help='Index to dump',
        ),
    )

    def handle(self, *args, **options):
        doc_types = ','.join(args) or None
        output = self.stdout
        output.write('[')
        es = connections.get_connection()
        for idx, doc in enumerate(scan(es, index=options['index'], doc_type=doc_types)):
            if idx > 0:
                output.write(',')
            output.write(json.dumps(doc, indent=options['indent']), ending='')
        output.write(']')
        output.flush()
