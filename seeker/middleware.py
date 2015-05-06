from django.db import models
from django.contrib.contenttypes.models import ContentType
from .utils import get_mappings, get_model_mappings
import operator
import logging

logger = logging.getLogger(__name__)

def find_relations(model_class, other_class, schema, prefix=''):
    """
    Given a parent model_class, a child other_class, and a seeker schema (field_name -> mapping_type), generates
    a list of relationship names that can be queried on model_class given an instance of other_class.
    """
    for name, t in schema.iteritems():
        if hasattr(t, 'model'):
            if t.model == other_class:
                # This field is a direct ObjectType reference to other_class, count it.
                yield prefix + name
            else:
                # Recurse through the schema of this ObjectType, in case one of its fields links to other_class.
                for n in find_relations(t.model, other_class, t.schema, prefix=prefix + name + '__'):
                    yield n
        else:
            try:
                # See if the model_class has a field named the same as the schema field, that is also a reference to other_class.
                f = model_class._meta.get_field(name)
                if f.rel.to == other_class:
                    yield prefix + name
            except:
                pass

def index_related(model_class, instance, delete=False):
    for mapping in get_mappings():
        if not mapping.auto_index:
            continue
        criteria = []
        for rel in find_relations(mapping.model, model_class, mapping.field_map):
            criteria.append(models.Q(**{rel: instance}))
            logger.debug('Relation found from %s to %s via "%s"', mapping.model.__name__, model_class.__name__, rel)
        if criteria:
            for obj in mapping.queryset().filter(reduce(operator.or_, criteria)):
                if delete:
                    mapping.delete(obj)
                else:
                    mapping.index(obj)

class ModelIndexingMiddleware (object):
    """
    Middleware class that automatically indexes any new or deleted model objects. ContentTypes are used
    in order to allow proper indexing of proxied models.
    """

    def __init__(self):
        models.signals.post_save.connect(self.handle_save)
        models.signals.post_delete.connect(self.handle_delete)

    def handle_save(self, sender, instance, **kwargs):
        model_class = ContentType.objects.get_for_model(instance).model_class()
        for mapping in get_model_mappings(model_class):
            if mapping.auto_index:
                mapping.index(instance)
        index_related(model_class, instance)

    def handle_delete(self, sender, instance, **kwargs):
        model_class = ContentType.objects.get_for_model(instance).model_class()
        for mapping in get_model_mappings(model_class):
            if mapping.auto_index:
                mapping.delete(instance)
        index_related(model_class, instance, delete=True)

    def process_request(self, request):
        # This is really just here so Django keeps the middleware installed.
        pass
