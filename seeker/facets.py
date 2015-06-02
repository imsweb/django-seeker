from elasticsearch_dsl import A, F
import functools
import operator

class Facet (object):
    field = None
    label = None
    aggregation = None
    template = None

    def __init__(self, field, label=None, name=None, template=None, **kwargs):
        self.field = field
        self.label = label or self.field.replace('_', ' ').replace('.raw', '').replace('.', ' ').capitalize()
        self.name = (name or self.field).replace('.', '_')
        self.template = template

    def filter(self, search, values):
        raise NotImplementedError('%s has not implemented a filter method.' % self.__class__.__name__)

    def apply(self, search):
        if self.aggregation:
            search.aggs[self.name] = self.aggregation
        return search

    def values(self, response):
        return response.aggregations[self.name]['buckets'] if self.aggregation else []

    def get_key(self, value):
        return value['key']

class TermsFacet (Facet):
    def __init__(self, field, label=None, template=None, size=10, **kwargs):
        super(TermsFacet, self).__init__(field, label=label, template=template, **kwargs)
        self.aggregation = A('terms', field=self.field, size=size)

    def filter(self, search, values):
        if len(values) > 1:
            kw = {self.field: values}
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

    def values(self, response):
        return response.aggregations[self.field][self.field]['buckets']

class YearHistogram (Facet):
    def __init__(self, field, label=None, template=None, fmt='yyyy'):
        super(YearHistogram, self).__init__(field, label=label, template=template)
        self.aggregation = A('date_histogram', field=self.field, interval='year', format=fmt, order={'_key': 'desc'})

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

    def get_key(self, value):
        return value['key_as_string']
