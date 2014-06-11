from django.apps import apps
from django.conf import settings

def get_app_mappings(app_label):
    seeker_app = apps.get_app_config('seeker')
    return seeker_app.app_mappings.get(app_label, [])

def get_model_mappings(model_class):
    seeker_app = apps.get_app_config('seeker')
    return seeker_app.model_mappings.get(model_class, [])

def get_facet_filters(request_data, facets, exclude=None):
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
    from elasticsearch import Elasticsearch
    from .query import Result
    seeker_app = apps.get_app_config('seeker')
    es = Elasticsearch(hosts or getattr(settings, 'SEEKER_HOSTS', None))
    query = query or {}
    if isinstance(query, basestring):
        query = {
            'query': {
                'query_string': {
                    'query': query,
                    'auto_generate_phrase_queries': True,
                    'analyze_wildcard': True,
                    'default_operator': getattr(settings, 'SEEKER_DEFAULT_OPERATOR', 'OR'),
                }
            }
        }
    response = es.search(index='_all', body=query)
    max_score = response['hits']['max_score']
    for hit in response['hits']['hits']:
        mapping = seeker_app.doc_types[hit['_type']]
        yield Result(mapping, hit, max_score)

def queryset(model_class, **kwargs):
    return get_model_mappings(model_class)[0].query(**kwargs).queryset()
