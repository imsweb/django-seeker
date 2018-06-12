from django.conf import settings
from elasticsearch_dsl import A, Q
from elasticsearch_dsl.aggs import Terms, Nested

import functools
import operator
import copy
import numbers

class Facet (object):
    bool_operators = {
        'not_equal': 'must_not',
        'not_between': 'must_not'
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

    def __init__(self, field, size=2147483647, **kwargs):
        # Elasticsearch default size to 10, so we set the default to 2147483647 in order to get all the buckets for the field.
        self.size = size
        self.filter_operator = kwargs.pop('filter_operator', 'or')
        super(TermsFacet, self).__init__(field, **kwargs)

    def _get_aggregation(self, **extra):
        params = {'field': self.field, 'size': self.size}
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


class DateTermsFacet(TermsFacet):
    def _get_aggregation(self, **extra):
        params = {
            'field': self.field,
            'interval': 'day',
            'format': 'MM/dd/yyyy',
            'min_doc_count': 1,
            'order': {'_key': 'desc'},
        }
        params.update(self.kwargs)
        params.update(extra)
        return A('date_histogram', **params)


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
        # ranges is an optional list of dictionaries of pre-defined ranges to aggregate on.
        self.ranges = kwargs.pop('ranges', [])
        
        # use_ranges_in_filter_dict specifies if the pre-defined ranges should be used in complex queries
        self.use_ranges_in_filter_dict = kwargs.pop('use_ranges_in_filter_dict', False)
        if self.ranges:
            # If the facet has pre-defined ranges, the facet is by default rendered as a select box with the defined ranges as values.
            # This can be overridden by passing advanced_template as a parameter in your facet definition.
            self.advanced_template = "advanced_seeker/facets/terms.html"
        super(RangeFilter, self).__init__(field, **kwargs)

    @property
    def valid_operators(self):
        # TODO: Add support for more operators: less, less or equal, greater, greater or equal
        operators = ['equal', 'not_equal']
        # between and not_between are valid operators if the facet doesn't have pre-defined ranges or if the facet specifies not to use the pre-defined ranges in the complex search.
        if not self.use_ranges_in_filter_dict or not self.ranges:
            operators = ['between', 'not_between'] + operators
        return operators

    def _get_aggregation(self, **extra):
        # If the facet has pre-defined ranges, this function will return a range aggregation.
        # If the facet doesn't have pre-defined ranges, this function will return a terms aggregation that aggregates on each value in the range field. 
        if self.ranges:
            params = {'field': self.field, 'ranges': self.ranges}
            params.update(extra)
            return A('range', **params)
        else:
            params = {'field': self.field}
            params.update(self.kwargs)
            params.update(extra)
            return A('terms', **params)

    def apply(self, search, **extra):
        search.aggs[self.name] = self._get_aggregation(**extra)
        return search
    
    def _get_range_key(self, range):
        """
        This helper function takes a range dictionary and returns the aggregation key.  Key is an optional argument in elasticsearch.
        If the range in self.ranges does not specify a key, the default key elasticsearch uses is "from-to", where from and to are either floats or *.
        """
        default_from_key = range.get("from", "*")
        default_to_key = range.get("to", "*")
        
        # If a from value was found, we need to cast it as a float since that's how elasticsearch formats the default key.
        if default_from_key != "*":
            default_from_key = float(default_from_key)
        # Same as above, if a to value is defined, cast the value as a float.
        if default_to_key != "*":
            default_to_key = float(default_to_key)
        
        # Create the default "from-to" key
        default_range_key = "{}-{}".format(default_from_key, default_to_key)
        
        # Return the custom key defined in self.ranges or the default key
        return str(range.get('key', default_range_key))

    def _get_filters_from_ranges(self, key):
        """
        This helper function takes a key(s) and finds the range boundaries that correspond to that key (defined in self.ranges).
        This function returns a list of filters.  One filter for each found key.
        
        This helper function is use by both seeker and advanced seeker:
            - In seeker, key will be a list of selected key values.
            - In advanced seeker, key will be a unicode representing 1 key from self.ranges.
        """
        # This is a list of filter query objects for each value range.
        filters = []

        # if key is equal to _missing, we create a query that returns every document that doesn't have a value for that field.
        if key == '_missing':
            filters.append(~Q('exists', field=self.field))
        else:
            # This function will only return filters for ranges that are defined in self.ranges (i.e - valid ranges)
            valid_ranges = []

            for range in self.ranges:
                # For each range in self.ranges, we get the key associated with that range so we can compare it to 'key'.
                range_key = self._get_range_key(range)

                # This if statement is structured to be cross compatible between seeker and advanced seeker.
                # If key is unicode (advanced seeker), we check if the key is equal to the range_key.  If it is, we add it to valid keys.
                # If key is a list (seeker), we check if the range_key is in the list of keys.  If it is, we add it to valid keys.
                if (isinstance(key, unicode) and range_key == key) or (isinstance(key, list) and range_key in key):
                    valid_ranges.append(range)
            for range in valid_ranges:
                # From and To are optional in elasticsearch.  The translated_range dictionary stores the parameters we
                # intend to use in our query base on what is defined in range.
                translated_range = {}
                if 'from' in range:
                    # We do greater-than or equal to because in Elasticsearch, a range aggregation includes the from value.
                    translated_range['gte'] = range['from']
                if 'to' in range:
                    # We do less-than because in Elasticsearch, a range aggregation exclude the to value.
                    # Doing less-than or equal to could cause the bucket count to be different than the result counts.
                    translated_range['lt'] = range['to']
                # We check that the range, defined in self.ranges, had a 'from' and/or a 'to' value.
                if translated_range:
                    filters.append(Q('range', **{self.field: translated_range}))
        return filters
    
    def _get_filter_from_range_list(self, range):
        """
        This helper function is designed to take a list of 2 values and build a range query.
        """
        if isinstance(range, dict):
            r = range
        elif isinstance(range, list):
            if len(range) == 2:
                r = {}
                # This function supports the ranges defined in range to either be a number or a str representation of a number.
                if isinstance(range[0], numbers.Number) or range[0].isdigit():
                    r['gte'] = range[0]
                if isinstance(range[1], numbers.Number) or range[1].isdigit():
                    r['lt'] = range[1]
            else:
                raise ValueError(u"The range list can only have 2 values. Received {} values: {}".format(len(range), range))
        else:
            raise ValueError(u"Range must either be a list or a dict.  Received: {}".format(type(range)))

        return Q('range', **{self.field: r})


    def es_query(self, query_operator, value):
        """
        This function returns the elasticsearch_dsl query object for the RangeFilter Facet.
        
        The "value" parameter will be 1 of three options:
            - list: value will be a list of two numbers. The first number represents the lower bound of the range and the second represents the upper bound of the range.
            - number: value can be single number.  Single numbers are used in complex queries when the operator is equal or not equal. (TODO: Update this comment once other operators are supported)
            - range key: value can represent a range key that defined in self.ranges
        
        """
        # We first check if the query operator is valid.
        if query_operator not in self.valid_operators:
            raise ValueError(u"'{}' is not a valid operator for a RangeFilter object.".format(query_operator))
        
        if isinstance(value, (list, dict)):
            # If value is a list defining the lower and upper bounds of the range, we call _get_filter_from_range_list that returns the DSL Filter object.
            filter = self._get_filter_from_range_list(value)
            query = Q('bool', filter=filter)
            # A check to see if the query should be wrapped in a parent query defined in self.bool_operators. If not, we return the query as-is. 
            if query_operator in self.bool_operators:
                return Q('bool', **{self.bool_operators[query_operator]: [query]})
            else:
                return query
        # We check if value is a number or a str/unicode representation of a number.
        elif isinstance(value, numbers.Number) or value.isdigit():
            # TODO: This logic will need to be updated when we support the operators: >, =>, <, and <=
            # If value is a number, the query will either be equal or not equal to the number stored in value.
            if query_operator in self.bool_operators:
                return Q('bool', **{self.bool_operators[query_operator]: [Q('term', **{self.field: value})]})
            return Q(self.special_operators.get(query_operator, 'term'), **{self.field: value})
        elif self.ranges:
            # We get a list of filter objects for each key defined in value.
            filters = self._get_filters_from_ranges(value)
            if filters:
                # Build a query object and add the list of filters to it.
                if query_operator in self.bool_operators:
                    return Q('bool', **{self.bool_operators[query_operator]: [Q('bool', filter=functools.reduce(operator.or_, filters))]})
                else:
                    return Q('bool', filter=functools.reduce(operator.or_, filters))
        else:
            raise ValueError("Received invalid range value. Value must be a list of two numbers, a number, or a key defined in self.ranges")

    def build_filter_dict(self, results):
        filter_dict = super(RangeFilter, self).build_filter_dict(results)
        if self.ranges and self.use_ranges_in_filter_dict:
            # If we have self.ranges, the filter is defaulted to a select box for those ranges 
            data = self.data(results)
            values = [''] + sorted([str(bucket['key']) for bucket in data['buckets']], key=lambda item: str(item).lower())
            filter_dict.update({
                'input': 'select',
                'values': values,
                'operators': self.valid_operators
            })
        else:
            filter_dict.update({'operators': self.valid_operators})
        return filter_dict

    def filter(self, search, values):
        if self.ranges:
            filters = self._get_filters_from_ranges(values)
            if filters:
                search = search.filter(functools.reduce(operator.or_, filters))
        else:
            search = search.filter(self._get_filter_from_range_list(values))
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


class DateRangeFacet(RangeFilter):
    advanced_template = 'advanced_seeker/facets/date_range.html'

    def __init__(self, field, format="MM/dd/yyyy", **kwargs):
        self.format = format
        super(DateRangeFacet, self).__init__(field, **kwargs)

    def _get_filter_from_range_list(self, range):
        range = super(DateRangeFacet, self)._get_filter_from_range_list(range)
        range._params[self.field]['format'] = self.format
        return range

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
