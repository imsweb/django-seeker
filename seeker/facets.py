from elasticsearch_dsl import A, F
import functools
import operator

class Facet (object):
    field = None
    label = None
    aggregation = None
    template = None

    def __init__(self, field, label=None, template=None):
        self.field = field
        self.label = label if label else self.field.replace('_', ' ').replace('.raw', '')
        self.template = template

    def filter(self, search, values):
        raise NotImplementedError()

    def apply(self, search):
        search.aggs[self.field] = self.aggregation
        return search

    def values(self, response):
        return response.aggregations[self.field]['buckets']

    def get_key(self, value):
        return value['key']

class TermsFacet (Facet):
    def __init__(self, field, label=None, template=None, size=10):
        super(TermsFacet, self).__init__(field, label=label, template=template)
        self.aggregation = A('terms', field=self.field, size=size)

    def filter(self, search, values):
        if len(values) > 1:
            kw = {self.field: values}
            return search.filter('terms', **kw)
        elif len(values) == 1:
            kw = {self.field: values[0]}
            return search.filter('term', **kw)
        return search

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
