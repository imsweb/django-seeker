from django.conf import settings
import collections
import operator
import logging
import copy

logger = logging.getLogger(__name__)

class Result (object):

    def __init__(self, mapping, hit, max_score=0, instance=None):
        self.mapping = mapping
        self.hit = hit
        self.max_score = float(max_score) if max_score is not None else 0.0
        self._instance = instance
        self._data = None
        self._highlight = None

    @property
    def data(self):
        if self._data is None:
            self._data = collections.OrderedDict()
            for name, t in self.mapping.field_map.iteritems():
                self._data[name] = t.to_python(self.hit['_source'].get(name))
        return self._data

    @property
    def highlight(self):
        if self._highlight is None:
            fields = self.hit.get('highlight', {})
            self._highlight = collections.OrderedDict()
            for name in self.mapping.field_map:
                if name in fields:
                    self._highlight[name] = fields[name][0]
                else:
                    self._highlight[name] = self.hit['_source'].get(name)
        return self._highlight

    @property
    def id(self):
        return self.hit['_id']

    @property
    def type(self):
        return self.hit['_type']

    @property
    def score(self):
        return float(self.hit['_score']) if self.hit['_score'] is not None else 0.0

    @property
    def percentile(self):
        return (self.score / self.max_score) * 100.0 if self.max_score > 0 else 0.0

    @property
    def instance(self):
        if self._instance is None:
            self._instance = self.mapping.queryset().get(pk=self.id)
        return self._instance

class ResultSet (object):

    def __init__(self, mapping, query=None, filters=None, facets=None, highlight=None, suggest=None, limit=10, offset=0, sort=None, prefetch=False):
        self.mapping = mapping
        self.query = query or {}
        if isinstance(self.query, basestring):
            self.query = {
                'query_string': {
                    'query': self.query,
                    'auto_generate_phrase_queries': True,
                    'analyze_wildcard': True,
                    'default_operator': getattr(settings, 'SEEKER_DEFAULT_OPERATOR', 'OR'),
                }
            }
        self.filters = filters or []
        if isinstance(self.filters, dict):
            self.filters = [F(**{name: values}) for name, values in self.filters.items()]
        elif isinstance(self.filters, F):
            self.filters = [self.filters]
        self.facets = facets or []
        if isinstance(self.facets, Aggregate):
            self.facets = [self.facets]
        self.highlight = highlight or {}
        if isinstance(self.highlight, (list, tuple)):
            self.highlight = {'fields': {f: {'number_of_fragments': 0} for f in self.highlight}}
        elif isinstance(self.highlight, basestring):
            self.highlight = {'fields': {self.highlight: {'number_of_fragments': 0}}}
        self.suggest = suggest
        if isinstance(self.suggest, basestring):
            self.suggest = {'suggest-all': {'text': self.suggest, 'term': {'field': '_all'}}}
        self.limit = limit
        self.offset = offset
        self.sort = sort or None
        if isinstance(self.sort, basestring):
            parts = self.sort.split(':', 1)
            name = parts[0]
            if name in mapping.field_map:
                f = mapping.field_map.get(name)
                d = {
                    'order': parts[1] if len(parts) > 1 else 'asc',
                    'ignore_unmapped': True,
                    'missing': '_last',
                }
                if f.index and f.data_type == 'string':
                    # For sorting on indexed strings, pull the value out of _source.
                    d['script'] = '_source.%s' % name
                    d['type'] = f.data_type
                    self.sort = [{'_script': d}]
                else:
                    self.sort = [{name: d}]
        self.prefetch = prefetch
        self._instances = {}
        self._response = None

    def to_elastic(self, for_count=False):
        if self.filters and not self.query and not self.facets and not for_count:
            # Fast case for filter-only searches. Can't be used for the count API, which expects a "query" key.
            f = reduce(operator.and_, self.filters)
            q = {'filter': f.to_elastic()}
        else:
            q = {}
            if self.filters:
                # If we have filters and either facets or a query, we need to use the filtered query.
                f = reduce(operator.and_, self.filters)
                q = {'query': {'filtered': {'filter': f.to_elastic()}}}
                if self.query:
                    q['query']['filtered']['query'] = self.query
            elif self.query:
                # Otherwise we can just use a straight query.
                q['query'] = self.query
            # Add any facets as aggregations.
            if self.facets:
                q['aggregations'] = {}
                for facet in self.facets:
                    q['aggregations'][facet.name] = facet.to_elastic()
        if self.highlight:
            q['highlight'] = self.highlight
        if self.suggest:
            q['suggest'] = self.suggest
        if self.sort:
            q['sort'] = self.sort
            q['track_scores'] = True
        return q or None

    def count(self):
        if self._response:
            return self.total
        else:
            query = self.to_elastic(for_count=True)
            logger.debug('Counting %s/%s: %s', self.mapping.index_name, self.mapping.doc_type, query)
            result = self.mapping.es.count(index=self.mapping.index_name, doc_type=self.mapping.doc_type, body=query)
            return result['count']

    @property
    def response(self):
        if self._response is None:
            query = self.to_elastic()
            logger.debug('Querying %s/%s: %s', self.mapping.index_name, self.mapping.doc_type, query)
            self._response = self.mapping.es.search(index=self.mapping.index_name, doc_type=self.mapping.doc_type, body=query, size=self.limit, from_=self.offset)
        return self._response

    @property
    def total(self):
        return self.response['hits']['total']

    @property
    def took(self):
        return self.response['took']

    @property
    def suggestions(self):
        try:
            suggs = {}
            for s in self.response['suggest']['suggest-all']:
                if s['options']:
                    suggs[s['text']] = s['options'][0]['text']
            return suggs
        except:
            return {}

    def __len__(self):
        return self.total

    def __iter__(self):
        max_score = self.response['hits']['max_score']
        if self.prefetch and not self._instances:
            pks = set(hit['_id'] for hit in self.response['hits']['hits'])
            for obj in self.mapping.queryset().filter(pk__in=pks):
                self._instances[obj.pk] = obj
        for hit in self.response['hits']['hits']:
            obj = self._instances.get(hit['_id'])
            yield Result(self.mapping, hit, max_score, instance=obj)

    def __getitem__(self, idx):
        max_score = self.response['hits']['max_score']
        hit = self.response['hits']['hits'][idx]
        obj = self._instances.get(hit['_id']) if self._instances else None
        return Result(self.mapping, hit, max_score, instance=obj)

    def facet_values(self):
        for facet in self.facets:
            yield facet, facet.facet_values(self.response)

    @property
    def aggregates(self):
        return collections.OrderedDict(self.facet_values())

class Aggregate (object):
    def __init__(self, field, name=None, label=None):
        self.field = field
        self.name = name or 'agg_%s' % self.field
        self.label = label or field.replace('_', ' ').capitalize()

    def facet_values(self, response):
        return response['aggregations'][self.name]['buckets']

    def to_elastic(self):
        raise NotImplementedError('Aggregate is not meant to be used directly; use a subclass instead')

    def filter(self):
        raise NotImplementedError('Aggregate is not meant to be used directly; use a subclass instead')

    def get_key(self, value):
        return value['key']

class TermAggregate (Aggregate):
    def __init__(self, field, name=None, label=None, size=10, include=None, exclude=None):
        super(TermAggregate, self).__init__(field, name=name, label=label)
        self.size = size
        self.include = include
        self.exclude = exclude

    def to_elastic(self):
        q = {'terms': {'field': self.field, 'size': self.size}}
        if self.include:
            q['terms']['include'] = {'pattern': self.include, 'flags': 'CASE_INSENSITIVE'}
        if self.exclude:
            q['terms']['exclude'] = {'pattern': self.exclude, 'flags': 'CASE_INSENSITIVE'}
        return q

    def filter(self, values):
        return F(**{self.field: values})

class StatsAggregate (Aggregate):
    def to_elastic(self):
        return {'stats': {'field': self.field}}

class YearHistogram (Aggregate):
    def to_elastic(self):
        return {'date_histogram': {'field': self.field, 'interval': 'year', 'format': 'yyyy', 'order': {'_key': 'desc'}}}

    def filter(self, values):
        return reduce(operator.or_, [Range(self.field, '%s-01-01' % val, '%s-12-31' % val) for val in values])

    def get_key(self, value):
        return value['key_as_string']

class RangeAggregate (Aggregate):
    def __init__(self, field, name=None, label=None, ranges=None, under='Under', over='Over', value_format=int):
        super(RangeAggregate, self).__init__(field, name=name, label=label)
        self.under = under
        self.over = over
        self.ranges = ranges
        self.value_format = value_format

    def to_elastic(self):
        return {'range': {'field': self.field, 'ranges': self.ranges}}

    def _parse_range(self, value):
        parts = value.split()
        if parts[0] == self.over:
            return self.value_format(parts[-1]), None
        elif parts[0] == self.under:
            return None, self.value_format(parts[-1])
        else:
            return self.value_format(parts[0]), self.value_format(parts[-1])

    def filter(self, values):
        ranges = []
        for val in values:
            min_value, max_value = self._parse_range(val)
            ranges.append(Range(self.field, min_value=min_value, max_value=max_value, max_oper='lt'))
        return reduce(operator.or_, ranges)

    def get_key(self, value):
        from_val = value.get('from', '')
        to_val = value.get('to', '')
        if from_val and to_val:
            return '%s - %s' % (self.value_format(from_val), self.value_format(to_val))
        elif from_val:
            return '%s %s' % (self.over, self.value_format(from_val))
        elif to_val:
            return '%s %s' % (self.under, self.value_format(to_val))
        return ''

# Basically taken from ElasticUtils, and simplified to generate Query DSL directly and not handle inverts.
# Ideally, I'd like to eventually just use ElasticUtils, but they don't support ES 1.0 yet.

class F (object):

    def __init__(self, **filters):
        filters = filters.items()
        if len(filters) > 1:
            self.filters = [{'and': filters}]
        else:
            self.filters = filters

    def __repr__(self):
        return '<F {0}>'.format(self.filters)

    def _combine(self, other, conn='and'):
        """
        OR and AND will create a new F, with the filters from both F
        objects combined with the connector `conn`.
        """
        f = F()

        self_filters = copy.deepcopy(self.filters)
        other_filters = copy.deepcopy(other.filters)

        if not self.filters:
            f.filters = other_filters
        elif not other.filters:
            f.filters = self_filters
        elif isinstance(self.filters[0], dict) and conn in self.filters[0]:
            f.filters = self_filters
            f.filters[0][conn].extend(other_filters)
        elif isinstance(other.filters[0], dict) and conn in other.filters[0]:
            f.filters = other_filters
            f.filters[0][conn].extend(self_filters)
        else:
            f.filters = [{conn: self_filters + other_filters}]

        return f

    def __or__(self, other):
        return self._combine(other, 'or')

    def __and__(self, other):
        return self._combine(other, 'and')

    def filter_spec(self, val):
        if isinstance(val[1], (basestring, bool, int)):
            return {'term': {val[0]: val[1]}}
        else:
            return {'terms': {val[0]: list(val[1])}}

    def to_elastic(self):
        def _es(val):
            if isinstance(val, dict):
                for conn, vals in val.items():
                    return {conn: [_es(v) for v in vals]}
            else:
                if hasattr(val, 'filter_spec'):
                    return val.filter_spec()
                else:
                    return self.filter_spec(val)
        if len(self.filters) > 1:
            return [_es(f) for f in self.filters]
        else:
            return _es(self.filters[0])

class RangeSpec (object):
    def __init__(self, field, min_value, max_value, min_oper='gte', max_oper='lte'):
        self.field = field
        self.min_value = min_value
        self.max_value = max_value
        self.min_oper = min_oper
        self.max_oper = max_oper

    def filter_spec(self):
        r = {}
        if self.min_value is not None:
            r[self.min_oper] = self.min_value
        if self.max_value is not None:
            r[self.max_oper] = self.max_value
        return {'range': {self.field: r}}

class Range (F):
    def __init__(self, field, min_value, max_value, min_oper='gte', max_oper='lte'):
        self.filters = [RangeSpec(field, min_value, max_value, min_oper=min_oper, max_oper=max_oper)]
