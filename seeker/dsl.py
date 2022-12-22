from django.conf import settings


if getattr(settings, 'SEEKER_OPENSEARCH_DSL', False):
    import opensearch_dsl as os_dsl
    from opensearch_dsl import A as os_A, Q as os_Q, Search as os_Search
    from opensearch_dsl.aggs import Terms as os_Terms
    from opensearch_dsl.connections import connections as os_connections
    from opensearch_dsl.field import Object as os_Object
    from opensearch_dsl.response import Response as os_Response
    from opensearch_dsl.utils import AttrList as os_AttrList
    from opensearchpy.exceptions import (
        AuthorizationException as os_AuthorizationException, NotFoundError as os_NotFoundError)
    from opensearchpy.helpers import bulk as os_bulk, scan as os_scan
    dsl = os_dsl
    AuthorizationException = os_AuthorizationException
    NotFoundError = os_NotFoundError
    bulk = os_bulk
    scan = os_scan
    A = os_A
    Q = os_Q
    Search= os_Search
    Terms = os_Terms
    connections = os_connections
    Object = os_Object
    Response = os_Response
    AttrList = os_AttrList
else:
    import elasticsearch_dsl as es_dsl
    from elasticsearch.exceptions import (
        AuthorizationException as es_AuthorizationException, NotFoundError as es_NotFoundError)
    from elasticsearch.helpers import bulk as es_bulk, scan as es_scan
    from elasticsearch_dsl import A as es_A, Q as es_Q, Search as as_Search
    from elasticsearch_dsl.aggs import Terms as es_Terms
    from elasticsearch_dsl.connections import connections as es_connections
    from elasticsearch_dsl.field import Object as es_Object
    from elasticsearch_dsl.response import Response as es_Response
    from elasticsearch_dsl.utils import AttrList as es_AttrList
    dsl = es_dsl
    AuthorizationException = es_AuthorizationException
    NotFoundError = es_NotFoundError
    bulk = es_bulk
    scan = es_scan
    A = es_A
    Q = es_Q
    Search= es_Search
    Terms = es_Terms
    connections = es_connections
    Object = es_Object
    Response = es_Response
    AttrList = es_AttrList
