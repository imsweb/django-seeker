__version__ = '0.2.0'

from .mapping import Indexable, document_from_model, document_field
from .facets import *
from .views import SeekerView
from .utils import get_app_mappings, get_model_mappings, get_facet_filters, index, crossquery, queryset
from .registry import register

default_app_config = 'seeker.apps.SeekerConfig'
