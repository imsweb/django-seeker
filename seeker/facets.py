from django.conf import settings
from elasticsearch_dsl import A, F
import functools
import operator

class Facet (object):
    field = None
    label = None
    template = getattr(settings, 'SEEKER_DEFAULT_FACET_TEMPLATE', 'seeker/facets/terms.html')

    def __init__(self, field, label=None, name=None, template=None, **kwargs):
        self.field = field
        self.label = label or self.field.replace('_', ' ').replace('.raw', '').replace('.', ' ').capitalize()
        self.name = (name or self.field).replace('.', '_')
        self.template = template or self.template

    def apply(self, search, **extra):
        return search

    def filter(self, search, values):
        return search

    def data(self, response):
        try:
            return response.aggregations[self.name]
        except:
            return {}

class TermsFacet (Facet):

    def __init__(self, field, **kwargs):
        self.size = kwargs.pop('size', 10)
        self.execution = kwargs.pop('execution', 'bool')
        super(TermsFacet, self).__init__(field, **kwargs)

    def apply(self, search, **extra):
        params = {'field': self.field, 'size': self.size}
        params.update(extra)
        search.aggs[self.name] = A('terms', **params)
        return search

    def filter(self, search, values):
        if len(values) > 1:
            kw = {self.field: values, 'execution': self.execution}
            return search.filter('terms', **kw)
        elif len(values) == 1:
            kw = {self.field: values[0]}
            return search.filter('term', **kw)
        return search

class GlobalTermsFacet (TermsFacet):

    def apply(self, search):
        top = A('global')
        top[self.field] = self.aggregation
        search.aggs[self.field] = top
        return search

    def data(self, response):
        return response.aggregations[self.field][self.field]

class YearHistogram (Facet):

    def __init__(self, field, **kwargs):
        self.fmt = kwargs.pop('fmt', 'yyyy')
        super(YearHistogram, self).__init__(field, **kwargs)

    def apply(self, search, **extra):
        params = {'field': self.field, 'interval': 'year', 'format': self.fmt, 'order': {'_key': 'desc'}}
        params.update(extra)
        search.aggs[self.name] = A('date_histogram', **params)
        return search

    def filter(self, search, values):
        filters = []
        for val in values:
            kw = {
                self.field: {
                    'gte': '%s-01-01' % val,
                    'lte': '%s-12-31' % val,
                }
            }
            filters.append(F('range', **kw))
        return search.filter(functools.reduce(operator.or_, filters))

class RangeFilter (Facet):
    template = 'seeker/facets/range.html'

    def filter(self, search, values):
        if len(values) == 2:
            r = {}
            if values[0].isdigit():
                r['gte'] = values[0]
            if values[1].isdigit():
                r['lte'] = values[1]
            search = search.filter('range', **{self.field: r})
        return search
