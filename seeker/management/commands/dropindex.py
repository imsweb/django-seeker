from django.conf import settings
from django.core.management.base import BaseCommand
from elasticsearch_dsl.connections import connections


class Command (BaseCommand):
    help = 'Drops the current ES index, or one that you specify.'

    def add_arguments(self, parser):
        parser.add_argument('--index',
            dest='index',
            default=None,
            help='The ES index to drop'
        )
        parser.add_argument('--using',
            dest='using',
            default=None,
            help='The ES connection alias to use'
        )

    def handle(self, *args, **options):
        index = options['index'] or getattr(settings, 'SEEKER_INDEX', 'seeker')
        connection = options['using'] or 'default'
        es = connections.get_connection(connection)

        print 'Attempting to drop index "%s" using "%s" connection...' % (index, connection)
        if es.indices.exists(index=index):
            es.indices.delete(index=index)
            if es.indices.exists(index=index):
                print '...The index was NOT dropped.'
            else:
                print '...The index was dropped.'
        else:
            print '...The index could not be dropped because it does not exist.'
