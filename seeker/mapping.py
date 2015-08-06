from django.conf import settings
from django.db import models
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.field import InnerObject
import elasticsearch_dsl as dsl
import logging
import six

logger = logging.getLogger(__name__)

def follow(obj, path, force_string=False):
    parts = path.split('__') if path else []
    for idx, part in enumerate(parts):
        if hasattr(obj, 'get_%s_display' % part):
            # If the root object has a method to get the display value for this part, we're done (the rest of the path, if any, is ignored).
            return getattr(obj, 'get_%s_display' % part)()
        else:
            # Otherwise, follow the yellow brick road.
            obj = getattr(obj, part, None)
            if isinstance(obj, models.Manager):
                # Managers are a special case - basically, branch and recurse over all objects with the remainder of the path. This means
                # any path with a Manager/ManyToManyField in it will always return a list, which I think makes sense.
                new_path = '__'.join(parts[idx + 1:])
                if new_path:
                    return [follow(o, new_path, force_string=True) for o in obj.all()]
    if force_string and isinstance(obj, models.Model):
        return six.text_type(obj)
    return obj

def serialize_object(obj, mapping, prepare=None):
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

class Indexable (dsl.DocType):

    @classmethod
    def documents(cls, **kwargs):
        return []

    @classmethod
    def count(cls):
        try:
            return len(cls.documents())
        except:
            return None

    @classmethod
    def clear(cls, index=None, using=None, keep_mapping=False):
        """
        Deletes the Elasticsearch mapping associated with this document type.
        """
        using = using or cls._doc_type.using or 'default'
        index = index or cls._doc_type.index or getattr(settings, 'SEEKER_INDEX', 'seeker')
        es = connections.get_connection(using)
        if es.indices.exists_type(index=index, doc_type=cls._doc_type.name):
            if keep_mapping:
                es.delete_by_query(index=index, doc_type=cls._doc_type.name, body={'query': {'match_all': {}}})
            else:
                es.indices.delete_mapping(index=index, doc_type=cls._doc_type.name)
            es.indices.flush(index=index)

class ModelIndex (Indexable):

    @classmethod
    def queryset(cls):
        raise NotImplementedError('%s must implement a queryset classmethod.' % cls.__name__)

    @classmethod
    def count(cls):
        return cls.queryset().count()

    @classmethod
    def documents(cls, **kwargs):
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
        return str(obj.pk)

    @classmethod
    def serialize(cls, obj):
        data = {'_id': cls.get_id(obj)}
        data.update(serialize_object(obj, cls._doc_type.mapping, prepare=cls))
        return data

    @property
    def instance(self):
        return self.queryset().get(pk=self.id)

RawString = dsl.String(analyzer='snowball', fields={
    'raw': dsl.String(index='not_analyzed'),
})

RawMultiString = dsl.String(analyzer='snowball', multi=True, fields={
    'raw': dsl.String(index='not_analyzed'),
})

def document_field(field):
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
                        extra=None):
    return type('%sDoc' % model_class.__name__, (document_class,), {
        'Meta': type('Meta', (object,), {
            'index': index or getattr(settings, 'SEEKER_INDEX', 'seeker'),
            'using': using,
            'mapping': build_mapping(model_class, doc_type=doc_type, fields=fields, exclude=exclude, field_factory=field_factory, extra=extra),
        }),
        'queryset': classmethod(lambda cls: model_class.objects.all()),
    })
