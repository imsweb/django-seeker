from elasticsearch_dsl import A, F
import operator

class BaseFacet (object):
    field = None
    label = None
    aggregation = None

    def __init__(self, field, label=None):
        self.field = field
        self.label = label if label else self.field.replace('_', ' ')

    def filter(self, search, values):
        raise NotImplementedError()

    def apply(self, search):
        search.aggs[self.field] = self.aggregation

    def values(self, response):
        return response.aggregations[self.field]['buckets']

    def get_key(self, value):
        return value['key']

class TermsFacet (BaseFacet):
    def __init__(self, field, label=None, size=10):
        super(TermsFacet, self).__init__(field, label=label)
        self.aggregation = A('terms', field=self.field, size=size)

    def filter(self, search, values):
        if len(values) > 1:
            kw = {self.field: values}
            return search.filter('terms', **kw)
        elif len(values) == 1:
            kw = {self.field: values[0]}
            return search.filter('term', **kw)
        return search

class YearHistogram (BaseFacet):
    def __init__(self, field, label=None, fmt='yyyy'):
        super(YearHistogram, self).__init__(field, label=label)
        self.aggregation = A('date_histogram', field=self.field, interval='year', format=fmt)

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
        return search.filter(reduce(operator.or_, filters))

    def get_key(self, value):
        return value['key_as_string']
