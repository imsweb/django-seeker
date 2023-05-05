from django.conf import settings


if getattr(settings, 'SEEKER_OPENSEARCH', False):
    import opensearch_dsl as dsl
    from opensearch_dsl import A, Q, Search
    from opensearch_dsl.aggs import Terms
    from opensearch_dsl.connections import connections
    from opensearch_dsl.field import Object
    from opensearch_dsl.response import Response
    from opensearch_dsl.utils import AttrList
    from opensearchpy.exceptions import AuthorizationException, NotFoundError, TransportError
    from opensearchpy.helpers import bulk, scan
else:
    import elasticsearch_dsl as dsl
    from elasticsearch.exceptions import AuthorizationException, NotFoundError, TransportError
    from elasticsearch.helpers import bulk, scan
    from elasticsearch_dsl import A, Q, Search
    from elasticsearch_dsl.aggs import Terms
    from elasticsearch_dsl.connections import connections
    from elasticsearch_dsl.field import Object
    from elasticsearch_dsl.response import Response
    from elasticsearch_dsl.utils import AttrList
