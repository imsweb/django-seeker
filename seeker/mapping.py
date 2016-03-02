from django.conf import settings
from django.db import models
from django.utils import timezone
from django.utils.text import capfirst
import elasticsearch
import collections
import datetime
import logging

logger = logging.getLogger(__name__)

def follow(obj, path):
    for part in path.split('__'):
        obj = getattr(obj, part, None)
    return obj

class MappingType (object):
    """
    The base class for all mapping types. Most options correspond to those documented at
    http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-core-types.html
    """

    data_type = 'string'
    """
    The Elasticsearch data type.
    """

    index = True
    """
    Whether the field should be indexed in Elasticsearch.
    """

    store = False
    """
    Whether the field should be stored in Elasticsearch. Defaults to False, since all fields are stored in ``_source`` by default anyway.
    """

    boost = None
    """
    A boost multiplier, used in scoring results.
    """

    include_in_all = None
    """
    Whether to include this field in the special Elasticsearch ``_all`` field, which is used when a search does not specify which fields to search.
    """

    multi = False
    """
    Whether this field
    """

    facet = False
    """
    Whether this field should be facetable when using the default seeker views.
    """

    def __init__(self, index=True, store=False, boost=None, include_in_all=None, facet=False, multi=False):
        self.index = index
        self.store = store
        self.boost = boost
        self.include_in_all = include_in_all
        self.facet = facet
        self.multi = multi

    def to_elastic(self, value):
        """
        Transforms a python value into a value suitable for sending to Elasticsearch. By default, returns a list of strings if the specified
        value has an ``all()`` method, otherwise returns the unicode representation of the value.
        """
        if value is None:
            return None
        elif isinstance(value, (list, tuple)):
            return [unicode(v) for v in value]
        elif hasattr(value, 'all'):
            return [unicode(v) for v in value.all()]
        else:
            return unicode(value)

    def to_python(self, value):
        """
        Coerces values coming out of Elasticsearch back to a python data type. By default, simply return the value from elasticsearch-py.
        """
        return value

    def mapping_params(self, **extra):
        """
        Returns a dictionary of mapping parameters to use when PUTing Elasticsearch mappings:
        http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-put-mapping.html
        """
        params = {
            'type': self.data_type,
        }
        if not self.index:
            params['index'] = 'no'
        if self.store:
            params['store'] = True
        if self.include_in_all is not None:
            params['include_in_all'] = self.include_in_all
        if self.boost and self.boost != 1:
            params['boost'] = self.boost
        params.update(extra)
        return params

class StringType (MappingType):
    """
    A string value. The __init__ method takes an optional additional parameter, ``analyzer``, that will be passed to Elasticsearch.
    If ``index`` is set to True, the string will be analyzed with the specified analyzer, and an additional ``.raw`` field will be
    stored as ``not_analyzed`` for sorting purposes.
    """

    data_type = 'string'

    def __init__(self, *args, **kwargs):
        self.analyzer = kwargs.pop('analyzer', 'snowball')
        self.include_raw = kwargs.pop('include_raw', True)
        super(StringType, self).__init__(*args, **kwargs)

    def mapping_params(self):
        extra = {'analyzer': self.analyzer} if self.index else {'index': 'not_analyzed'}
        if self.index and self.include_raw:
            extra['fields'] = {'raw': {'type': 'string', 'index': 'not_analyzed'}}
        return super(StringType, self).mapping_params(**extra)

class DateType (MappingType):
    """
    A date value with an ES format of ``date``.
    """

    data_type = 'date'

    def to_elastic(self, value):
        if value is None:
            return None
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d')
        return value

    def to_python(self, value):
        try:
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()
        except:
            logger.warning('Could not parse date value: %s', value)
            return value

    def mapping_params(self):
        return super(DateType, self).mapping_params(format='date')

class DateTimeType (MappingType):
    """
    A date value with an ES format of ``date_optional_time``.
    """

    data_type = 'date'

    def to_elastic(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            if not timezone.is_aware(value):
                logger.warning('Mapping a naive datetime (%s); assuming local time', value)
                value = timezone.make_aware(value, timezone=timezone.get_default_timezone())
            # Send the value to Elasticsearch in UTC.
            return timezone.localtime(value, timezone=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        elif hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d')
        return value

    def _parse_datetime(self, value):
        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
            try:
                return datetime.datetime.strptime(value, fmt)
            except:
                pass
        raise ValueError('No matching date format was found.')

    def to_python(self, value):
        try:
            # Dates coming out of Elasticsearch will be in UTC, make them aware and convert them to local time.
            d = timezone.make_aware(self._parse_datetime(value), timezone=timezone.utc)
            return timezone.localtime(d)
        except:
            logger.warning('Could not parse datetime value: %s', value)
            return value

    def mapping_params(self):
        return super(DateTimeType, self).mapping_params(format='date_optional_time')

class BooleanType (MappingType):
    """
    A boolean value.
    """

    data_type = 'boolean'

    def to_elastic(self, value):
        if value is None:
            return None
        return bool(value)

class IntegerType (MappingType):
    """
    An integer value.
    """

    data_type = 'integer'

    def to_elastic(self, value):
        if value is None:
            return None
        return int(value)

class FloatType (MappingType):
    """
    A float value, actually stored in ES as ``double``. Currently, this is used for both Django ``FloatField`` and ``DecimalField`` (which
    may lose some precision).
    """

    data_type = 'double'

    def to_elastic(self, value):
        if value is None:
            return None
        return float(value)

DEFAULT_TYPE_MAP = {
    models.CharField: StringType,
    models.TextField: StringType(include_raw=False),
    models.SlugField: StringType(index=False),
    models.EmailField: StringType,
    models.ForeignKey: StringType(facet=True),
    models.DateField: DateType,
    models.DateTimeField: DateTimeType,
    models.BooleanField: BooleanType(facet=True),
    models.NullBooleanField: BooleanType(facet=True),
    models.ManyToManyField: StringType(facet=True),
    models.IntegerField: IntegerType,
    models.PositiveIntegerField: IntegerType,
    models.FloatField: FloatType,
    models.DecimalField: FloatType,
}

try:
    from django.contrib.postgres import fields
    DEFAULT_TYPE_MAP[fields.ArrayField] = StringType(index=False, multi=True)
except ImportError:
    pass

def object_data(obj, schema, preparer=None):
    """
    Helper function for converting a Django object into a dictionary based on the specified schema (name -> MappingType).
    If a preparer is specified, it will be searched for prepare_<fieldname> methods.
    """
    data = {}
    for name, t in schema.iteritems():
        if preparer and hasattr(preparer, 'prepare_%s' % name):
            data[name] = getattr(preparer, 'prepare_%s' % name)(obj)
        elif hasattr(obj, 'get_%s_display' % name):
            data[name] = getattr(obj, 'get_%s_display' % name)()
        else:
            try:
                data[name] = t.to_elastic(follow(obj, name))
            except:
                logger.exception('Problem extracting data for %s', name)
    return data

class ObjectType (MappingType):

    def __init__(self, model=None, fields=None, exclude=None, **schema):
        self.schema = {}
        self.model = model
        if self.model is not None:
            for f in (self.model._meta.fields + self.model._meta.many_to_many):
                if f.__class__ in DEFAULT_TYPE_MAP and (fields is None or f.name in fields) and (exclude is None or f.name not in exclude):
                    t = DEFAULT_TYPE_MAP[f.__class__]
                    if f.choices:
                        # Special case for Django fields with choices.
                        t = StringType(index=False)
                    if isinstance(t, type):
                        t = t()
                    self.schema[f.name] = t
        for name, t in schema.iteritems():
            if isinstance(t, type):
                t = t()
            self.schema[name] = t

    def to_elastic(self, value):
        if hasattr(value, 'all'):
            return [object_data(obj, self.schema, preparer=self) for obj in value.all()]
        elif hasattr(value, 'pk'):
            return object_data(value, self.schema, preparer=self)
        return None

    def mapping_params(self):
        return {'properties': {name: t.mapping_params() for name, t in self.schema.iteritems()}}

class Mapping (object):
    """
    A mapping is an interface for translating python objects into ElasticSearch data.
    """

    model = None
    """
    A Django model class to hook this mapping up to. If specified, Seeker will automatically scan
    the model for fields to index (unless fields is specified), and index all instances of this model.
    """

    fields = None
    """
    This may be a dict mapping field name to a MappingType instance, or a list of field names to include.
    By default, all indexable model fields are included.
    """

    exclude = None
    """
    A list of fields to exclude when automatically generating mapping types from the model.
    """

    overrides = None
    """
    A dict mapping field names you want to override to MappingType instances. Field names listed here do
    not need to be actual model field names.
    """

    field_label_overrides = None
    """
    A dict mapping field names to labels for display.
    """

    batch_size = None
    """
    Batch size to use when indexing large querysets. Defaults to SEEKER_BATCH_SIZE.
    """

    auto_index = True
    """
    Set to False to disable re-indexing on save/delete signals.
    """

    type_map = DEFAULT_TYPE_MAP
    """
    A mapping from Django field class to a MappingType class or instance. This is used when specifying model,
    but not fields.
    """

    def __init__(self):
        self._field_cache = None
        if self.batch_size is None:
            self.batch_size = getattr(settings, 'SEEKER_BATCH_SIZE', 1000)

    @property
    def hosts(self):
        """
        A list of ElasticSearch hosts to connect to when indexing and querying this mapping.
        Defaults to the :ref:`SEEKER_HOSTS <setting-seeker-hosts>` setting.
        """
        return getattr(settings, 'SEEKER_HOSTS', None)

    @property
    def es(self):
        if not hasattr(self, '_es'):
            self._es = elasticsearch.Elasticsearch(self.hosts, **self.connection_options)
        return self._es

    @property
    def doc_type(self):
        """
        A property defining the type name in ElasticSearch. Defaults to the lowercased name of the class with
        "mapping" stripped off, so MyTypeMapping becomes just "mytype".
        """
        return self.__class__.__name__.lower().replace('mapping', '')

    @property
    def index_name(self):
        """
        The name of the index data for this mapping should be stored in.
        Defaults to the :ref:`SEEKER_INDEX <setting-seeker-index>` setting.
        """
        return getattr(settings, 'SEEKER_INDEX', 'seeker')

    @property
    def connection_options(self):
        """
        A dictionary of keyword arguments to use when creating an Elasticsearch instance. By default, this will
        include ``http_auth`` if the ``SEEKER_HTTP_AUTH`` setting is specified.
        """
        auth = getattr(settings, 'SEEKER_HTTP_AUTH', None)
        return {'http_auth': auth} if auth else {}

    def build_schema(self):
        return {
            '_all': {'enabled': True, 'analyzer': 'snowball'},
            'dynamic': 'strict',
            'properties': {name: t.mapping_params() for name, t in self.field_map.iteritems()},
        }

    def _get_field(self, name, t):
        if self.overrides and name in self.overrides:
            t = self.overrides[name]
        if isinstance(t, type):
            t = t()
        return t

    def get_fields(self):
        """
        An iterator yielding field names and MappingType object instances describing them.
        """
        seen = set()
        if isinstance(self.fields, dict):
            for name, t in self.fields.iteritems():
                seen.add(name)
                yield name, self._get_field(name, t)
        else:
            for f in (self.model._meta.fields + self.model._meta.many_to_many):
                if f.__class__ in self.type_map and (self.fields is None or f.name in self.fields) and (self.exclude is None or f.name not in self.exclude):
                    t = self.type_map[f.__class__]
                    if f.choices:
                        # Special case for Django fields with choices.
                        t = StringType(index=False)
                    seen.add(f.name)
                    yield f.name, self._get_field(f.name, t)
        if isinstance(self.overrides, dict):
            for name, t in self.overrides.iteritems():
                if name not in seen:
                    if isinstance(t, type):
                        t = t()
                    yield name, t

    @property
    def field_map(self):
        if self._field_cache is None:
            self._field_cache = collections.OrderedDict()
            for name, t in self.get_fields():
                self._field_cache[name] = t
        return self._field_cache

    def field_label(self, field_name):
        """
        Returns a human-readable label for the given field name, found in the following order:
        - Label set in self.field_label_overrides.
        - If the field name comes from a Django model, the verbose_name is looked up.
        - The field name is transformed by replacing underscores with spaces.
        """
        if self.field_label_overrides and field_name in self.field_label_overrides:
            return self.field_label_overrides[field_name]
        try:
            return capfirst(self.model._meta.get_field(field_name).verbose_name)
        except:
            return ' '.join(w.capitalize() for w in field_name.split('_'))

    @property
    def field_labels(self):
        return collections.OrderedDict((name, self.field_label(name)) for name in self.field_map)

    def queryset(self):
        """
        The queryset to use when indexing or fetching model instances for this mapping. Defaults to ``self.model.objects.all()``.
        A common use for overriding this method would be to add ``select_related()`` or ``prefetch_related()``.
        """
        return self.model.objects.all()

    def should_index(self, obj):
        """
        Called by :meth:`.get_objects` for every object returned by :meth:`.queryset` to determine if it should be indexed. The default
        implementation simply returns `True` for every object.
        """
        return True

    def get_objects(self, cursor=False):
        """
        A generator yielding object instances that will subsequently be indexed using :meth:`.get_data` and :meth:`.get_id`. This method
        calls :meth:`.queryset` and orders it by ``pk``, then slices the results according to :attr:`.batch_size`. This results
        in more queries, but avoids loading all objects into memory at once.

        :param cursor: If True, use a server-side cursor when fetching the results for better performance.
        """
        if cursor:
            from .compiler import CursorQuery
            qs = self.queryset().order_by()
            # Swap out the Query object with a clone using our subclass.
            qs.query = qs.query.clone(klass=CursorQuery)
            for obj in qs.iterator():
                if self.should_index(obj):
                    yield obj
        else:
            qs = self.queryset().order_by('pk')
            total = qs.count()
            for start in range(0, total, self.batch_size):
                end = min(start + self.batch_size, total)
                for obj in qs.all()[start:end]:
                    if self.should_index(obj):
                        yield obj

    def get_id(self, obj):
        """
        Returns an ID for ElasticSearch to use when indexing the specified object. Defaults to ``obj.pk``. Must be unique over :attr:`doc_type`.
        """
        return unicode(obj.pk)

    def get_data(self, obj):
        """
        Returns a dictionary mapping field names to values. Values are generated by first "following" any relations (i.e. traversing __ field notation),
        then calling :meth:`MappingType.to_elastic` on the resulting value.
        """
        return object_data(obj, self.field_map, preparer=self)

    def index(self, obj, refresh=True):
        """
        Send a single object to Elasticsearch for indexing.
        """
        if self.should_index(obj):
            self.es.index(index=self.index_name, doc_type=self.doc_type, id=self.get_id(obj), body=self.get_data(obj), refresh=refresh)

    def delete(self, obj, refresh=True):
        """
        Delete a single object from the Elasticsearch index.
        """
        try:
            self.es.delete(index=self.index_name, doc_type=self.doc_type, id=self.get_id(obj), refresh=refresh)
        except elasticsearch.TransportError, e:
            # Ignore 404 errors here, since the record doesn't exist anyway.
            if e.status_code != 404:
                raise e

    def refresh(self):
        """
        Creates the Elasticsearch index if necessary, and PUTs the mapping parameters.
        """
        self._field_cache = None
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name)
        self.es.indices.put_mapping(index=self.index_name, doc_type=self.doc_type, body=self.build_schema())
        self.es.indices.flush(index=self.index_name)

    def clear(self):
        """
        Clears the Elasticsearch index by deleting the mapping entirely.
        """
        if self.es.indices.exists_type(index=self.index_name, doc_type=self.doc_type):
            self.es.indices.delete_mapping(index=self.index_name, doc_type=self.doc_type)
            self.es.indices.flush(index=self.index_name)

    def query(self, **kwargs):
        """
        Returns a :class:`ResultSet` for this mapping with the specified parameters.
        """
        from .query import ResultSet
        return ResultSet(self, **kwargs)

    @classmethod
    def instance(cls):
        cache_name = '_seeker_instance_%s' % cls.__name__
        if not hasattr(cls, cache_name):
            setattr(cls, cache_name, cls())
        return getattr(cls, cache_name)
