from django.db import models
from django.contrib.contenttypes.models import ContentType
from .utils import get_model_mappings

class ModelIndexingMiddleware (object):
    """
    Middleware class that automatically indexes any new or deleted model objects. ContentTypes are used
    in order to allow proper indexing of proxied models.
    """

    def __init__(self):
        models.signals.post_save.connect(self.handle_save)
        models.signals.post_delete.connect(self.handle_delete)

    def handle_save(self, sender, instance, **kwargs):
        ct = ContentType.objects.get_for_model(instance)
        for mapping in get_model_mappings(ct.model_class()):
            if mapping.auto_index:
                mapping.index(instance)

    def handle_delete(self, sender, instance, **kwargs):
        ct = ContentType.objects.get_for_model(instance)
        for mapping in get_model_mappings(ct.model_class()):
            if mapping.auto_index:
                mapping.delete(instance)

    def process_request(self, request):
        # This is really just here so Django keeps the middleware installed.
        pass
