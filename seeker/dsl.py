from django.conf import settings

import elasticsearch_dsl as es_dsl
import opensearch_dsl as os_dsl
from elasticsearch.exceptions import AuthorizationException as es_AuthorizationException
from elasticsearch.exceptions import NotFoundError as es_NotFoundError
from elasticsearch.helpers import bulk as es_bulk
from elasticsearch.helpers import scan as es_scan
from elasticsearch_dsl import A as es_A
from elasticsearch_dsl import Q as es_Q
from elasticsearch_dsl.aggs import Terms as es_Terms
from elasticsearch_dsl.connections import connections as es_connections
from elasticsearch_dsl.field import Object as es_Object
from elasticsearch_dsl.utils import AttrList as es_AttrList
from opensearch_dsl import A as os_A
from opensearch_dsl import Q as os_Q
from opensearch_dsl.aggs import Terms as os_Terms
from opensearch_dsl.connections import connections as os_connections
from opensearch_dsl.field import Object as os_Object
from opensearch_dsl.utils import AttrList as os_AttrList
from opensearchpy.exceptions import AuthorizationException as os_AuthorizationException
from opensearchpy.exceptions import NotFoundError as os_NotFoundError
from opensearchpy.helpers import bulk as os_bulk
from opensearchpy.helpers import scan as os_scan


class ElasticSearchDSL:
    dsl = es_dsl
    A = es_A
    Q = es_Q
    Terms = es_Terms
    connections = es_connections
    Object = es_Object
    AttrList = es_AttrList
    AuthorizationException = es_AuthorizationException
    NotFoundError = es_NotFoundError
    bulk = es_bulk
    scan = es_scan


class OpenSearchDSL:
    dsl = os_dsl
    A = os_A
    Q = os_Q
    Terms = os_Terms
    connections = os_connections
    Object = os_Object
    AttrList = os_AttrList
    AuthorizationException = os_AuthorizationException
    NotFoundError = os_NotFoundError
    bulk = os_bulk
    scan = os_scan


SEEKER_DSL = getattr(settings, 'SEEKER_DSL', ElasticSearchDSL)
dsl = SEEKER_DSL.dsl
A = SEEKER_DSL.A
Q = SEEKER_DSL.Q
Terms = SEEKER_DSL.Terms
connections = SEEKER_DSL.connections
Object = SEEKER_DSL.Object
AttrList = SEEKER_DSL.AttrList
AuthorizationException = SEEKER_DSL.AuthorizationException
NotFoundError = SEEKER_DSL.NotFoundError
bulk = SEEKER_DSL.bulk
scan = SEEKER_DSL.scan
