from django.conf import settings
from elasticsearch_dsl import A, Q
from elasticsearch_dsl.aggs import Terms, Nested

import functools
import operator
import copy


class Facet (object):
    bool_operators = {
        'not_equal': 'must_not',
    }
    special_operators = {
        'begins_with': "prefix" 
    }
    field = None
    label = None
    template = getattr(settings, 'SEEKER_DEFAULT_FACET_TEMPLATE', 'seeker/facets/terms.html')
    advanced_template = getattr(settings, 'ADVANCED_SEEKER_DEFAULT_FACET_TEMPLATE', 'advanced_seeker/facets/terms.html')

    def __init__(self, field, label=None, name=None, description=None, template=None, advanced_template=None, **kwargs):
        self.field = field
        self.label = label or self.field.replace('_', ' ').replace('.raw', '').replace('.', ' ').capitalize()
        self.name = (name or self.field).replace('.raw', '').replace('.', '_')
        self.template = template or self.template
        self.advanced_template = advanced_template or self.advanced_template
        self.description = description
        self.kwargs = kwargs
        
    @property
    def valid_operators(self):
        return [
            'equal',
            'not_equal',
            'begins_with'
        ]

    def apply(self, search, **extra):
        """
        This function applies an aggregation to the 'search' parameter for this facet.
        """
        return search

    def filter(self, search, values):
        return search
    
    def es_query(self, operator, value):
        """
        This function returns the elasticsearch_dsl query object for this facet. It only accepts a single value, multiple values
        will need to be combined together outside of this function.
        """
        if operator not in self.valid_operators:
            raise ValueError(u"'{}' is not a valid operator for the {} facet.".format(operator, self.label))
        
        if operator in self.bool_operators:
            return Q('bool', **{self.bool_operators[operator]: [Q('match', **{self.field: value})]})
        return Q(self.special_operators.get(operator, 'match'), **{self.field: value})
    
    def build_filter_dict(self, results):
        """
        This function returns a dictionary that represents this facet.
        The dictionary this returns is designed to be compatible with the JQuery Querybuilder plugin (http://querybuilder.js.org/index.html#filters).
        When overwriting this function please pass all applicable values defined in that plugin.
        """
        return {
            'id': self.field,
            'label': self.label,
            'type': 'string'
        }

    def data(self, response):
        try:
            return response.aggregations[self.name].to_dict()
        except:
            return {}

    def get_key(self, bucket):
        return bucket.get('key')

    def get_facet_sort_key(self, bucket):
        return self.get_key(bucket).lower()

    def buckets(self, response):
        for b in self.data(response).get('buckets', []):
            yield self.get_key(b), b.get('doc_count')


class TermsFacet (Facet):

    def __init__(self, field, **kwargs):
        self.filter_operator = kwargs.pop('filter_operator', 'or')
        super(TermsFacet, self).__init__(field, **kwargs)

    def _get_aggregation(self, **extra):
        params = {'field': self.field}
        params.update(self.kwargs)
        params.update(extra)
        return A('terms', **params)

    def apply(self, search, **extra):
        search.aggs[self.name] = self._get_aggregation(**extra)
        return search

    def es_query(self, operator, value):
        """
        This function returns the elasticsearch_dsl query object for this facet. It only accepts a single value and is designed for use with the
        'complex query' functionality.
        """
        if operator not in self.valid_operators:
            raise ValueError(u"'{}' is not a valid operator for a TermsFacet object.".format(operator))
        
        if operator in self.bool_operators:
            return Q('bool', **{self.bool_operators[operator]: [Q('term', **{self.field: value})]})
        return Q(self.special_operators.get(operator, 'term'), **{self.field: value})
    
    def build_filter_dict(self, results):
        filter_dict = super(TermsFacet, self).build_filter_dict(results)
        data = self.data(results)
        values = [''] + sorted([str(bucket['key']) for bucket in data['buckets']], key=lambda item: str(item).lower())
        filter_dict.update({
            'input': 'select',
            'values': values,
            'operators': self.valid_operators
        })
        return filter_dict
    
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

    def apply(self, search, **extra):
        top = A('global')
        top[self.field] = self._get_aggregation(**extra)
        search.aggs[self.field] = top
        return search

    def data(self, response):
        return response.aggregations[self.field][self.field].to_dict()


class YearHistogram (Facet):
    template = 'seeker/facets/year_histogram.html'
    advanced_template = 'advanced_seeker/facets/year_histogram.html'

    def apply(self, search, **extra):
        params = {
            'field': self.field,
            'interval': 'year',
            'format': 'yyyy',
            'min_doc_count': 1,
            'order': {'_key': 'desc'},
        }
        params.update(self.kwargs)
        params.update(extra)
        search.aggs[self.name] = A('date_histogram', **params)
        return search

    def filter(self, search, values):
        filters = []
        for val in values:
            kw = {
                self.field: {
                    'gte': '%s-01-01T00:00:00' % val,
                    'lte': '%s-12-31T23:59:59' % val,
                }
            }
            filters.append(Q('range', **kw))
        return search.query(functools.reduce(operator.or_, filters))

    def get_key(self, bucket):
        return bucket.get('key_as_string')


class RangeFilter (Facet):
    template = 'seeker/facets/range.html'
    advanced_template = 'advanced_seeker/facets/range.html'
    
    def __init__(self, field, **kwargs):
        self.ranges = kwargs.pop('ranges', [])
        self.missing = kwargs.pop('missing', -1)
        super(RangeFilter, self).__init__(field, **kwargs)

    @property
    def valid_operators(self):
        return [
            'equal',
            'not_equal'
        ]

    def _get_aggregation(self, **extra):
        params = {'field': self.field, 'ranges': self.ranges, 'missing':self.missing}
        params.update(extra)
        return A('range', **params)

    def apply(self, search, **extra):
        if self.ranges:
            search.aggs[self.name] = self._get_aggregation(**extra)
        return search

    def _get_filters(self, value):
        valid_ranges = []
        # We only accept ranges that are defined
        for range in self.ranges:
            range_value = str(range.get('key'))
            if (isinstance(value, unicode) and range_value == value) or (isinstance(value, list) and range_value in value):
                valid_ranges.append(range)
        filters = []
        for range in valid_ranges:
            if 'from' in range and range['from'] == self.missing:
                filters.append(~Q('exists', field=self.field))
            else:
                translated_range = {}
                if 'from' in range:
                    translated_range['gte'] = range['from']
                if 'to' in range:
                    translated_range['lt'] = range['to']
                if translated_range:
                    filters.append(Q('range', **{self.field: translated_range}))
        return filters

    def es_query(self, query_operator, value):
        """
        This function returns the elasticsearch_dsl query object for this facet. It only accepts a single value and is designed for use with the
        'complex query' functionality.
        """
        if query_operator not in self.valid_operators:
            raise ValueError(u"'{}' is not a valid operator for a RangeFilter object.".format(query_operator))
        
        if self.ranges:
            filters = self._get_filters(value)
            if filters:
                if query_operator in self.bool_operators:
                    return Q('bool', **{self.bool_operators[query_operator]: [Q('bool', filter=functools.reduce(operator.or_, filters))]})
                else:
                    return Q('bool', filter=functools.reduce(operator.or_, filters))

    def build_filter_dict(self, results):
        filter_dict = super(RangeFilter, self).build_filter_dict(results)
        data = self.data(results)
        values = [''] + sorted([str(bucket['key']) for bucket in data['buckets']], key=lambda item: str(item).lower())
        filter_dict.update({
            'input': 'select',
            'values': values,
            'operators': self.valid_operators
        })
        return filter_dict

    def filter(self, search, values):
        if self.ranges:
            filters = self._get_filters(values)
            if filters:
                search = search.filter(functools.reduce(operator.or_, filters))
        else:
            if len(values) == 2:
                r = {}
                if values[0].isdigit():
                    r['gte'] = values[0]
                if values[1].isdigit():
                    r['lte'] = values[1]
                search = search.filter('range', **{self.field: r})
        return search

    def data(self, response, values=[], **kwargs):
        try:
            facet_data = response.aggregations[self.name].to_dict()
            buckets = copy.deepcopy(facet_data['buckets'])
            for bucket in buckets:
                if bucket['key'] not in values and bucket['doc_count'] == 0:
                    facet_data['buckets'].remove(bucket)
            if kwargs.get('sort_facets', True) and 'buckets' in facet_data:
                facet_data['buckets'] = sorted(facet_data['buckets'], key=self.get_facet_sort_key)
            return facet_data
        except:
            return {}

    def in_range(self, range_key, value):
        for range in self.ranges:
            if range['key'] == range_key:
                if 'from' in range and range['from'] > value:
                    return False
                if 'to' in range and range['to'] <= value:
                    return False
                return True
        return False

class NestedFacet (Facet):
    template = 'seeker/facets/nested.html'

    @property
    def valid_operators(self):
        return [
            'equal',
            'not_equal',
        ]

    def __init__(self, path, field, **kwargs):
        self.path = path
        super(NestedFacet, self).__init__(field, **kwargs)

    def _get_nested_bucket(self):
        return Nested(path=self.path)

    def _get_aggregation(self):
        return Terms(field=self.field)

    def apply(self, search):
        search.aggs.bucket(self.name, self._get_nested_bucket()).bucket(self.name, self._get_aggregation())
        return search

    def filter(self, search, values):
        if values:
            nested_kwargs = {'query': Q("terms", **{self.field: values}), "path": self.path}
            return search.filter("nested", **nested_kwargs)
        return search
