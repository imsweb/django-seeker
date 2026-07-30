import json
from optparse import make_option

from django.core.management.base import BaseCommand, CommandError
from seeker import bulk, connections


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--filename',
            dest='filename',
            default=None,
            help='The file to load index data from',
        )
        parser.add_argument(
            '--index',
            dest='index',
            default=None,
            help='Index to load data into',
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

        connection = connections.get_connection()
        bulk(connection, get_actions())

        for index in refresh_indices:
            connection.indices.refresh(index=index)
