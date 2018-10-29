from django.apps import apps
from django.conf import settings


def get_mappings():
    """
    Returns a list of all defined mappings.
    """
    seeker_app = apps.get_app_config('seeker')
    return seeker_app.mappings


def get_app_mappings(app_label):
    """
    Returns a list of mappings for the specified ``app_label``.
    """
    seeker_app = apps.get_app_config('seeker')
    return seeker_app.app_mappings.get(app_label, [])


def get_model_mappings(model_class):
    """
    Returns a list of mappings for the specified model class.
    """
    seeker_app = apps.get_app_config('seeker')
    return seeker_app.model_mappings.get(model_class, [])


def get_search_query_type(query):
    query_type = getattr(settings, 'SEEKER_QUERY_TYPE', 'query_string')
    query_type_config = {
        query_type: {
            'query': query,
            'analyze_wildcard': True,
            'default_operator': getattr(settings, 'SEEKER_DEFAULT_OPERATOR', 'OR'),
        }
    }
    if query_type == 'query_string':
        query_type_config[query_type]['auto_generate_phrase_queries'] = True
    return query_type_config


def get_facet_filters(request_data, facets, exclude=None):
    """
    Given request data (i.e. ``request.GET`` or ``request.POST``) and a list of facets (``Aggregate`` subclasses),
    returns a dictionary of filtered terms (facet.field -> [term1, term2]) and a list of :class:`seeker.query.F`
    instances suitable for filtering queries.
    """
    filters = {}
    facet_filters = []
    if exclude is None:
        exclude = set()
    for facet in facets:
        if facet.field in request_data and facet.field not in exclude:
            terms = request_data.getlist(facet.field)
            filters[facet.field] = set(terms)
            facet_filters.append(facet.filter(terms))
    return filters, facet_filters


def index(obj):
    """ Shortcut to index an object based on it's model class. """
    # TODO: should this use ContentType, to deal with proxy models?
    for mapping in get_model_mappings(obj.__class__):
        mapping.index(obj)


def crossquery(query, suggest=None, limit=None, offset=None, hosts=None):
    """
    Yields Result objects matching the given query across all Elasticsearch indices.
    """
    from elasticsearch import Elasticsearch
    from .query import Result
    seeker_app = apps.get_app_config('seeker')
    es = Elasticsearch(hosts or getattr(settings, 'SEEKER_HOSTS', None))
    query = query or {}
    if isinstance(query, str):
        query = {
            'query': get_search_query_type(query)
        }
    response = es.search(index='_all', body=query)
    max_score = response['hits']['max_score']
    for hit in response['hits']['hits']:
        mapping = seeker_app.doc_types[hit['_type']]
        yield Result(mapping, hit, max_score)


def queryset(model_class, **kwargs):
    """
    Given a model class and keyword query arguments, returns a Django QuerySet by first querying Elasticsearch for a list of
    IDs, then calling ``mapping.queryset().filter(pk__in=<id_list>)``.
    """
    return get_model_mappings(model_class)[0].query(**kwargs).queryset()
