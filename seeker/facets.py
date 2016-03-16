from django.conf import settings
from elasticsearch_dsl import A, Q
import functools
import operator

class Facet (object):
    field = None
    label = None
    template = getattr(settings, 'SEEKER_DEFAULT_FACET_TEMPLATE', 'seeker/facets/terms.html')

    def __init__(self, field, label=None, name=None, template=None, **kwargs):
        self.field = field
        self.label = label or self.field.replace('_', ' ').replace('.raw', '').replace('.', ' ').capitalize()
        self.name = (name or self.field).replace('.raw', '').replace('.', '_')
        self.template = template or self.template

    def apply(self, search, **extra):
        return search

    def filter(self, search, values):
        return search

    def data(self, response):
        try:
            return response.aggregations[self.name].to_dict()
        except:
            return {}

class TermsFacet (Facet):

    def __init__(self, field, **kwargs):
        self.size = kwargs.pop('size', 10)
        self.filter_operator = kwargs.pop('filter_operator', 'or')
        super(TermsFacet, self).__init__(field, **kwargs)

    def apply(self, search, **extra):
        params = {'field': self.field, 'size': self.size}
        params.update(extra)
        search.aggs[self.name] = A('terms', **params)
        return search

    def filter(self, search, values):
        if len(values) > 1:
            if self.filter_operator.lower() == 'and':
                filters = [Q('term', **{self.field: v}) for v in values]
                return search.query(functools.reduce(operator.and_, filters))
            else:
                return search.filter('terms', **{self.field: values})
        elif len(values) == 1:
            return search.filter('term', **{self.field: values[0]})
        return search

class GlobalTermsFacet (TermsFacet):

    def apply(self, search):
        top = A('global')
        top[self.field] = self.aggregation
        search.aggs[self.field] = top
        return search

    def data(self, response):
        return response.aggregations[self.field][self.field].to_dict()

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
            filters.append(Q('range', **kw))
        return search.query(functools.reduce(operator.or_, filters))

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
