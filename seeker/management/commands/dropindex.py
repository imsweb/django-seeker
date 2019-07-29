from django.conf import settings
from django.core.management.base import BaseCommand
from elasticsearch_dsl.connections import connections
from elasticsearch.exceptions import AuthorizationException, NotFoundError
from django.core.exceptions import ImproperlyConfigured

class Command (BaseCommand):
    help = 'Drops all ES indexes on project with SEEKER_INDEX_PREFIX from settings, or one that you specify. To drop indexes with prefix add wildcard * after prefix of indexes you want deleted'

    def add_arguments(self, parser):
        parser.add_argument('--index',
            dest='index',
            default=None,
            help='The ES index(es) to drop'
        )
        parser.add_argument('--using',
            dest='using',
            default=None,
            help='The ES connection alias to use'
        )

    def handle(self, *args, **options):
        if options['index']:
            index_prefix = options['index']
        else:
            seeker_index_prefix = getattr(settings, 'SEEKER_INDEX_PREFIX', None)
            if seeker_index_prefix:
                index_prefix = '{}*'.format(seeker_index_prefix)
            else:
                raise ImproperlyConfigured('An index or index prefix must be supplied (either through --index or SEEKER_INDEX_PREFIX setting)')
        connection = options['using'] or 'default'
        es = connections.get_connection(connection)
        print('Using connection: "{}"'.format(connection))
        print('Attempting to drop index(es) using the pattern: {}'.format(index_prefix))
        for index in es.indices.get(index_prefix):
            try:
                print('Attempting to drop index "{}"...'.format(index))
                if es.indices.exists(index=index):
                    es.indices.delete(index=index)
                    if es.indices.exists(index=index):
                        print('...The index was NOT dropped.')
                    else:
                        print('...The index was dropped.')
                else:
                    print('...The index could not be dropped because it does not exist.')
            except AuthorizationException:
                print('You are not authorized to drop index: "{}")'.format(index_prefix))
        print('Done. Please verify statements above for success/failure.')
