__version__ = '3.0-dev'

from .facets import Facet, GlobalTermsFacet, RangeFilter, TermsFacet, YearHistogram
from .mapping import (
    DEFAULT_ANALYZER, Indexable, ModelIndex, RawMultiString, RawString, build_mapping, deep_field_factory,
    document_field, document_from_model)
from .registry import app_documents, documents, model_documents, register
from .utils import delete, index, search
from .views import Column, SeekerView

def get_app_name():
    from django.conf import settings
    return getattr(settings, 'DJANGO_SEEKER_APP_NAME', 'seeker')

def get_app_label():
    from django.conf import settings
    return getattr(settings, 'DJANGO_SEEKER_APP_LABEL', 'seeker')
    
default_app_config = 'seeker.apps.SeekerConfig'
