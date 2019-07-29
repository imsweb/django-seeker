__version__ = '6.1.1'

from .facets import DateRangeFacet, DateTermsFacet, Facet, GlobalTermsFacet, RangeFilter, TermsFacet, YearHistogram, TextFacet
from .mapping import (
    build_mapping, deep_field_factory, DEFAULT_ANALYZER, document_field, document_from_model, Indexable, index_factory, ModelIndex,
    RawMultiString, RawString)
from .registry import app_documents, documents, model_documents, register
from .utils import delete, index, search
from .views import AdvancedColumn, AdvancedSavedSearchView, AdvancedSeekerView, Column, SeekerView


default_app_config = 'seeker.apps.SeekerConfig'
