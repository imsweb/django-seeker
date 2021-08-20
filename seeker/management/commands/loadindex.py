import json
from optparse import make_option

from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from elasticsearch.helpers import bulk
from elasticsearch_dsl.connections import connections



class Command(BaseCommand):
    args = '<app1 app2 ...>'
    option_list = BaseCommand.option_list + (
        make_option('--filename', '-f',
                    dest='filename',
                    default=None,
                    help='The file to load index data from',
        ),
        make_option('--index',
                    dest='index',
                    default=None,
                    help='Index to load data into',
        ),
    )

    def handle(self, *args, **options):
        if not options['filename']:
            raise CommandError('Please specify a file (-f) to read data from')

        refresh_indices = set()

        def get_actions():
            for data in json.load(open(options['filename'], 'rb')):
                if options['index']:
                    data['_index'] = options['index']
                refresh_indices.add(data['_index'])
                yield data

        es = connections.get_connection()
        bulk(es, get_actions())

        for index in refresh_indices:
            es.indices.refresh(index=index)
