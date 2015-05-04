from django.conf import settings
from django.db import models
from elasticsearch_dsl.connections import connections
import elasticsearch_dsl as dsl
import logging
import six

logger = logging.getLogger(__name__)

def follow(obj, path):
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
                return [follow(o, new_path) for o in obj.all()]
    # We traversed the whole path and wound up with an object. If it's a Django model, use the unicode representation.
    if isinstance(obj, models.Model):
        return six.text_type(obj)
    return obj

class Indexable (object):
    
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
    def label_for_field(cls, field_name):
        """
        Returns a human-readable label for the given field name.
        """
        return field_name.replace('_', ' ').capitalize()

    @classmethod
    def clear(cls, using=None, index=None):
        """
        Deletes the Elasticsearch mapping associated with this document type.
        """
        if index is None:
            index = cls._doc_type.index
        es = connections.get_connection(using or cls._doc_type.using)
        if es.indices.exists_type(index=index, doc_type=cls._doc_type.name):
            es.indices.delete_mapping(index=index, doc_type=cls._doc_type.name)
            es.indices.flush(index=index)

class ModelIndex (Indexable):

    @classmethod
    def queryset(cls):
        raise NotImplementedError()

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
    def serialize(cls, obj):
        data = {'_id': str(obj.pk)}
        for name in cls._doc_type.mapping:
            data[name] = follow(obj, name)
        return data

    @classmethod
    def label_for_field(cls, field_name):
        if field_name.endswith('.raw'):
            field_name = field_name[:-4]
        try:
            f = cls.queryset().model._meta.get_field(field_name)
            return f.verbose_name.capitalize()
        except:
            return super(ModelIndex, cls).label_for_field(field_name)

    @property
    def instance(self):
        return self.queryset().get(pk=self.id)

#RawString = dsl.String(analyzer='snowball', fields={
#    'raw': dsl.String(index='not_analyzed'),
#})
RawString = dsl.String(index='not_analyzed', fields={
    'analyzed': dsl.String(analyzer='snowball', store=True, include_in_all=True),
})

def document_field(field):
    defaults = {
        models.DateField: dsl.Date(),
        models.DateTimeField: dsl.Date(),
        models.IntegerField: dsl.Long(),
        models.BooleanField: dsl.Boolean(),
        models.NullBooleanField: dsl.Boolean(),
        models.SlugField: dsl.String(index='not_analyzed'),
    }
    s = dsl.String(analyzer='snowball', fields={
        'raw': dsl.String(index='not_analyzed'),
    })
    return defaults.get(field.__class__, s)

def document_from_model(model_class, document_class=dsl.DocType, fields=None, exclude=None,
                        index=None, using='default', doc_type=None, mapping=None, field_factory=None):
    meta_parent = (object,)
    if hasattr(document_class, 'Meta'):
        meta_parent = (document_class.Meta, object)
    if index is None:
        index = getattr(settings, 'SEEKER_INDEX', 'seeker')
    if doc_type is None:
        doc_type = model_class.__name__.lower()
    if mapping is None:
        mapping = dsl.Mapping(doc_type)
    attrs = {
        'Meta': type('Meta', meta_parent, {
            'index': index,
            'using': using,
            'doc_type': doc_type,
            'mapping': mapping,
        }),
    }
    if field_factory is None:
        field_factory = document_field
    for f in model_class._meta.fields + model_class._meta.many_to_many:
        if not isinstance(f, models.AutoField):
            field = field_factory(f)
            if field:
                attrs[f.name] = field
    return type('%sDoc' % model_class.__name__, (document_class, ModelIndex), attrs)
