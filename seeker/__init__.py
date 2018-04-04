__version__ = '3.0-dev'

from .facets import Facet, GlobalTermsFacet, RangeFilter, TermsFacet, YearHistogram, NestedFacet
from .mapping import (
    DEFAULT_ANALYZER, Indexable, ModelIndex, RawMultiString, RawString, build_mapping, deep_field_factory,
    document_field, document_from_model)
from .registry import app_documents, documents, model_documents, register
from .utils import delete, index, search
from .views import Column, SeekerView, AdvancedSeekerView, AdvancedSavedSearchView


default_app_config = 'seeker.apps.SeekerConfig'
