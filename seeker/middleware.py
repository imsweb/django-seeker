from django.db import models

from .utils import delete, index

import logging


logger = logging.getLogger(__name__)


class ModelIndexingMiddleware (object):
    """
    Middleware class that automatically indexes any new or deleted model objects.
    """

    def __init__(self, get_response=None):
        models.signals.post_save.connect(self.handle_save)
        models.signals.post_delete.connect(self.handle_delete)
        self.get_response = get_response

    def __call__(self, request):
        response = None
        if hasattr(self, 'process_request'):
            response = self.process_request(request)
        if not response:
            response = self.get_response(request)
        if hasattr(self, 'process_response'):
            response = self.process_response(request, response)
        return response

    def handle_save(self, sender, instance, **kwargs):
        try:
            index(instance)
        except:
            logger.exception('Error indexing %s instance: %s', sender, instance)

    def handle_delete(self, sender, instance, **kwargs):
        try:
            delete(instance)
        except:
            logger.exception('Error deleting %s instance: %s', sender, instance)

    def process_request(self, request):
        # This is really just here so Django keeps the middleware installed.
        pass
