import logging

import elasticsearch_dsl as dsl
import six
from django.conf import settings
from django.db import models
from elasticsearch.helpers import bulk, scan
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.field import InnerObject


logger = logging.getLogger(__name__)


DEFAULT_ANALYZER = getattr(settings, 'SEEKER_DEFAULT_ANALYZER', 'snowball')


def follow(obj, path, force_string=False):
    parts = path.split('__') if path else []
    for idx, part in enumerate(parts):
        if hasattr(obj, 'get_%s_display' % part):
            # If the root object has a method to get the display value for this part, we're done (the rest of the path,
            # if any, is ignored).
            return getattr(obj, 'get_%s_display' % part)()
        else:
            # Otherwise, follow the yellow brick road.
            obj = getattr(obj, part, None)
            if isinstance(obj, models.Manager):
                # Managers are a special case - basically, branch and recurse over all objects with the remainder of the
                # path. This means any path with a Manager/ManyToManyField in it will always return a list, which I
                # think makes sense.
                new_path = '__'.join(parts[idx + 1:])
                if new_path:
                    return [follow(o, new_path, force_string=True) for o in obj.all()]
    if force_string and isinstance(obj, models.Model):
        return six.text_type(obj)
    return obj


def serialize_object(obj, mapping, prepare=None):
    """
    Given a Django model instance and a ``elasticsearch_dsl.Mapping`` or ``elasticsearch_dsl.InnerObject``, returns a
    dictionary of field data that should be indexed.
    """
    data = {}
    for name in mapping:
        prep_func = getattr(prepare, 'prepare_%s' % name, None)
        if prep_func:
            data[name] = prep_func(obj)
        else:
            field = mapping[name]
            value = follow(obj, name)
            if value is not None:
                if isinstance(value, models.Model):
                    data[name] = serialize_object(value, field.properties) if isinstance(field, InnerObject) else six.text_type(value)
                elif isinstance(value, models.Manager):
                    if isinstance(field, InnerObject):
                        data[name] = [serialize_object(v, field.properties) for v in value.all()]
                    else:
                        data[name] = [six.text_type(v) for v in value.all()]
                else:
                    data[name] = value
    return data


class Indexable(dsl.DocType):
    """
    An ``elasticsearch_dsl.DocType`` subclass with methods for getting a list (and count) of documents that should be
    indexed.
    """

    class Meta:
        index = getattr(settings, 'SEEKER_INDEX', 'seeker')

    @classmethod
    def documents(cls, **kwargs):
        """
        Returns (or yields) a list of documents, which are dictionaries of field data. The documents may include
        Elasticsearch metadata, such as ``_id`` or ``_parent``.
        """
        return []

    @classmethod
    def count(cls):
        """
        Returns the number of elements returned by ``Indexable.documents``. May be overridden for performance reasons.
        """
        try:
            return len(cls.documents())
        except Exception:
            return None

    @classmethod
    def clear(cls, index=None, using=None):
        """
        Deletes the Elasticsearch mapping associated with this document type.
        """
        using = using or cls._doc_type.using or 'default'
        index = index or cls._doc_type.index or getattr(settings, 'SEEKER_INDEX', 'seeker')
        es = connections.get_connection(using)
        if es.indices.exists_type(index=index, doc_type=cls._doc_type.name):
            def get_actions():
                for hit in scan(es, index=index, doc_type=cls._doc_type.name, query={'query': {'match_all': {}}}):
                    yield {
                        '_op_type': 'delete',
                        '_index': index,
                        '_type': cls._doc_type.name,
                        '_id': hit['_id'],
                    }
            bulk(es, get_actions())
            es.indices.refresh(index=index)


class ModelIndex(Indexable):
    """
    A subclass of ``Indexable`` that returns document data based on Django models.
    """

    @classmethod
    def queryset(cls):
        """
        Must be overridden to return a QuerySet of objects that should be indexed. Ideally, the QuerySet should have
        select_related and prefetch_related specified for any relationships that will be traversed during indexing.
        """
        raise NotImplementedError('%s must implement a queryset classmethod.' % cls.__name__)

    @classmethod
    def count(cls):
        """
        Overridden to return ``cls.queryset().count()``.
        """
        return cls.queryset().count()

    @classmethod
    def documents(cls, **kwargs):
        """
        Yields document data generated from ``cls.queryset()``. Multiple queries are made, fetching
        ``SEEKER_BATCH_SIZE`` instances at a time, to avoid memory problems with very large querysets.

        :param cursor: If ``True``, a special ``CustorQuery`` will be used to yield Django objects from a server-side
                       cursor.
        """
        if kwargs.get('cursor', False):
            from .compiler import CursorQuery
            qs = cls.queryset().order_by()
            # Swap out the Query object with a clone using our subclass.
            qs.query = qs.query.clone(klass=CursorQuery)
            for obj in qs.iterator():
                yield cls.serialize(obj)
        else:
            qs = cls.queryset().order_by('pk')
            total = qs.count()
            batch_size = getattr(settings, 'SEEKER_BATCH_SIZE', 1000)
            for start in range(0, total, batch_size):
                end = min(start + batch_size, total)
                for obj in qs.all()[start:end]:
                    yield cls.serialize(obj)

    @classmethod
    def get_id(cls, obj):
        """
        Returns the Elasticsearch ``_id`` to use for the specified model instance. Defaults to ``str(obj.pk)``.
        """
        return str(obj.pk)

    @classmethod
    def serialize(cls, obj):
        """
        Returns a dictionary of field data for the specified model instance. Also includes an ``_id`` which is returned
        from ``cls.get_id(obj)``. Uses ``seeker.mapping.serialize_object`` to build the field data dictionary.
        """
        data = {'_id': cls.get_id(obj)}
        data.update(serialize_object(obj, cls._doc_type.mapping, prepare=cls))
        return data

    @classmethod
    def connect_additional_signal_handlers(cls, indexer):
        """
            Override to register additional signal handlers to work in conjunction with ``SEEKER_INDEXER`` 
            (e.g. if a mapping includes related data such as a ManyToManyField, a signal could be registered to index 
            on the ManyToManyField save). 
        """
        pass

    @classmethod
    def disconnect_additional_signal_handlers(cls, indexer):
        pass

    @property
    def instance(self):
        """
        Returns the Django model instance corresponding to this document, fetched using ``cls.queryset()``.
        """
        return self.queryset().get(pk=self.meta.id)

RawString = dsl.String(analyzer=DEFAULT_ANALYZER, fields={
    'raw': dsl.String(index='not_analyzed'),
})
"""
An ``elasticsearch_dsl.String`` instance (analyzed using ``SEEKER_DEFAULT_ANALYZER``) with a ``raw`` sub-field that is
not analyzed, suitable for aggregations, sorting, etc.
"""

RawMultiString = dsl.String(analyzer=DEFAULT_ANALYZER, multi=True, fields={
    'raw': dsl.String(index='not_analyzed'),
})
"""
The same as ``RawString``, but with ``multi=True`` specified, so lists are returned.
"""


def document_field(field):
    """
    The default ``field_factory`` method for converting Django field instances to ``elasticsearch_dsl.Field`` instances.
    Auto-created fields (primary keys, for example) and one-to-many fields (reverse FK relationships) are skipped.
    """
    if field.auto_created or field.one_to_many:
        return None
    if field.many_to_many:
        return RawMultiString
    defaults = {
        models.DateField: dsl.Date(),
        models.DateTimeField: dsl.Date(),
        models.IntegerField: dsl.Long(),
        models.PositiveIntegerField: dsl.Long(),
        models.BooleanField: dsl.Boolean(),
        models.NullBooleanField: dsl.Boolean(),
        models.SlugField: dsl.String(index='not_analyzed'),
        models.DecimalField: dsl.Double(),
        models.FloatField: dsl.Float(),
    }
    return defaults.get(field.__class__, RawString)


def deep_field_factory(field):
    if field.is_relation and (field.many_to_one or field.one_to_one):
        props = {}
        for f in field.related_model._meta.get_fields():
            nested_field = deep_field_factory(f)
            if nested_field is not None:
                props[f.name] = nested_field
        return dsl.Object(properties=props)
    else:
        return document_field(field)


def build_mapping(model_class, mapping=None, doc_type=None, fields=None, exclude=None, field_factory=None, extra=None):
    """
    Defines Elasticsearch fields for Django model fields. By default, this method will create a new
    ``elasticsearch_dsl.Mapping`` object with fields corresponding to the ``model_class``.

    :param model_class: The Django model class to build a mapping for
    :param mapping: An ``elasticsearch_dsl.Mapping`` or ``elasticsearch_dsl.InnerObject`` instance to define fields on
    :param doc_type: The doc_type to use, if no mapping is specified
    :param fields: A list of Django model field names to include
    :param exclude: A list of Django model field names to exclude
    :param field_factory: A function that takes a Django model field instance, and returns a ``elasticsearch_dsl.Field``
    :param extra: A dictionary (field_name -> ``elasticsearch_dsl.Field``) of extra fields to include in the mapping
    """
    if mapping is None:
        if doc_type is None:
            doc_type = model_class.__name__.lower()
        mapping = dsl.Mapping(doc_type)
    if field_factory is None:
        field_factory = document_field
    for f in model_class._meta.get_fields():
        if fields and f.name not in fields:
            continue
        if exclude and f.name in exclude:
            continue
        field = field_factory(f)
        if field is not None:
            mapping.field(f.name, field)
    if extra:
        for name, field in extra.items():
            mapping.field(name, field)
    return mapping


def document_from_model(model_class, document_class=ModelIndex, fields=None, exclude=None,
                        index=None, using='default', doc_type=None, field_factory=None,
                        extra=None, module='seeker.mappings'):
    """
    Returns an instance of ``document_class`` with a ``Meta`` inner class and default ``queryset`` class method.
    """
    return type('%sDoc' % model_class.__name__, (document_class,), {
        'Meta': type('Meta', (object,), {
            'index': index or getattr(settings, 'SEEKER_INDEX', 'seeker'),
            'using': using,
            'mapping': build_mapping(model_class, doc_type=doc_type, fields=fields, exclude=exclude, field_factory=field_factory, extra=extra),
        }),
        'queryset': classmethod(lambda cls: model_class.objects.all()),
        '__module__': module,
    })
