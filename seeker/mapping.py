from django.conf import settings
from django.db import models
from django.utils import timezone
import collections
import datetime
import logging

logger = logging.getLogger(__name__)

def follow(obj, path):
    for part in path.split('__'):
        obj = getattr(obj, part, None)
    return obj

class MappingType (object):
    data_type = 'string'
    index = True
    store = False
    boost = None
    include_in_all = None
    multi = False
    facet = False

    def __init__(self, index=True, store=False, boost=None, include_in_all=None, facet=False, multi=False):
        self.index = index
        self.store = store
        self.boost = boost
        self.include_in_all = include_in_all
        self.facet = facet
        self.multi = multi

    def to_elastic(self, value):
        if value is None:
            return None
        try:
            return [unicode(v) for v in value.all()]
        except:
            return unicode(value)

    def to_python(self, value):
        return value

    def mapping_params(self, **extra):
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
    data_type = 'string'

    def mapping_params(self):
        extra = {'analyzer': 'snowball'} if self.index else {'index': 'not_analyzed'}
        return super(StringType, self).mapping_params(**extra)

class DateType (MappingType):
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
    data_type = 'date'

    def to_elastic(self, value):
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            if timezone.is_aware(value):
                return timezone.localtime(value, timezone=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
            else:
                logger.warning('Mapping a naive datetime (%s); assuming local time', value)
                return timezone.make_aware(value, timezone=timezone.get_default_timezone()).strftime('%Y-%m-%dT%H:%M:%S')
        elif hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d')
        return value

    def to_python(self, value):
        try:
            for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
                return datetime.datetime.strptime(value, fmt)
            raise ValueError('No matching date format was found.')
        except:
            logger.warning('Could not parse datetime value: %s', value)
            return value

    def mapping_params(self):
        return super(DateTimeType, self).mapping_params(format='date_optional_time')

class BooleanType (MappingType):
    data_type = 'boolean'

    def to_elastic(self, value):
        return bool(value)

class IntegerType (MappingType):
    data_type = 'integer'

    def to_elastic(self, value):
        return int(value)

DEFAULT_TYPE_MAP = {
    models.CharField: StringType,
    models.TextField: StringType,
    models.SlugField: StringType(index=False),
    models.EmailField: StringType,
    models.ForeignKey: StringType(index=False, facet=True),
    models.DateField: DateType,
    models.DateTimeField: DateTimeType,
    models.BooleanField: BooleanType(facet=True),
    models.NullBooleanField: BooleanType(facet=True),
    models.ManyToManyField: StringType(index=False, facet=True),
    models.IntegerField: IntegerType,
    models.PositiveIntegerField: IntegerType,
}

def object_data(obj, schema, preparer=None):
    """
    Helper function for converting a Django object into a dictionary based on the specified schema (name -> MappingType).
    If a preparer is specified, it will be search for prepare_<fieldname> methods.
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
                pass
    return data

class ObjectType (MappingType):

    def __init__(self, model=None, fields=None, exclude=None, **schema):
        self.schema = {}
        if model is not None:
            for f in (model._meta.fields + model._meta.many_to_many):
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
            return [object_data(obj, self.schema) for obj in value.all()]
        elif hasattr(value, 'pk'):
            return object_data(value, self.schema)
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

    batch_size = getattr(settings, 'SEEKER_BATCH_SIZE', 1000)
    """
    Batch size to use when indexing large querysets.
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
            from elasticsearch import Elasticsearch
            self._es = Elasticsearch(self.hosts)
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

    def build_schema(self):
        return {
            '_all': {'enabled': True, 'analyzer': 'snowball'},
            'dynamic': 'strict',
            'properties': {name: t.mapping_params() for name, t in self.field_map.items()},
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
            for name, t in self.fields.items():
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
        if self.overrides:
            for name, t in self.overrides.items():
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
            for obj in qs:
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

    def index(self, obj):
        self.es.index(index=self.index_name, doc_type=self.doc_type, id=self.get_id(obj), body=self.get_data(obj))

    def delete(self, obj):
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=self.get_id(obj))

    def refresh(self):
        self._field_cache = None
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name)
        self.es.indices.put_mapping(index=self.index_name, doc_type=self.doc_type, body=self.build_schema())
        self.es.indices.flush(index=self.index_name)

    def clear(self):
        if self.es.indices.exists_type(index=self.index_name, doc_type=self.doc_type):
            self.es.indices.delete_mapping(index=self.index_name, doc_type=self.doc_type)
            self.es.indices.flush(index=self.index_name)

    def query(self, **kwargs):
        from .query import ResultSet
        return ResultSet(self, **kwargs)

    @classmethod
    def instance(cls):
        if not hasattr(cls, '_seeker_singleton'):
            cls._seeker_singleton = cls()
        return cls._seeker_singleton
