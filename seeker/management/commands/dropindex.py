from django.conf import settings
from django.core.management.base import BaseCommand
from elasticsearch_dsl.connections import connections
from elasticsearch.exceptions import AuthorizationException, NotFoundError

class Command (BaseCommand):
    help = 'Drops all ES indexes on project with SEEKER_INDEX_PREFIX from settings, or one that you specify. To drop indexes with prefix add wildcard * after prefix of indexes you want deleted'

    def add_arguments(self, parser):
        parser.add_argument('--index',
            dest='index',
            default=None,
            help='The ES index(ex) to drop'
        )
        parser.add_argument('--using',
            dest='using',
            default=None,
            help='The ES connection alias to use'
        )

    def handle(self, *args, **options):
        try:
            index_prefix = options['index'] or getattr(settings, 'SEEKER_INDEX_PREFIX', None) + '*'
        except TypeError:
            print "Index not correctly defined, define SEEKER_INDEX_PREFIX in settings correctly to drop all "
        connection = options['using'] or 'default'
        es = connections.get_connection(connection)
        try:
            for index in es.indices.get(index_prefix):
                print 'Attempting to drop index "%s" using "%s" connection...' % (index, connection)
                if es.indices.exists(index=index):
                    es.indices.delete(index=index)
                    if es.indices.exists(index=index):
                        print '...The index was NOT dropped.'
                    else:
                        print '...The index was dropped.'
                else:
                    print '...The index could not be dropped because it does not exist.'
        except NotFoundError:
            print 'No index with that name found'
        except AuthorizationException:
            print 'You are not authorized to drop that index! (index: %s)' % index_prefix
