import logging
import warnings

from django.db import models

from .utils import delete, index


logger = logging.getLogger(__name__)


class ModelIndexingMiddleware(object):
    """
    Deprecated: Middleware class that automatically indexes any new or deleted model objects.
    """

    def __init__(self, get_response=None):
        self.get_response = get_response
        warnings.warn("ModelIndexingMiddleware is deprecated for seeker. Please utilize seeker.SEEKER_INDEXER setting instead.", DeprecationWarning)

    def __call__(self, request):
        response = None
        if hasattr(self, 'process_request'):
            response = self.process_request(request)
        if not response:
            response = self.get_response(request)
        if hasattr(self, 'process_response'):
            response = self.process_response(request, response)
        return response

    def process_request(self, request):
        # This is really just here so Django keeps the middleware installed.
        pass
