__version__ = '0.2.0'

from .mapping import Indexable, document_from_model, document_field
from .facets import Facet, TermsFacet, YearHistogram
from .views import SeekerView
from .utils import get_mappings, search, index, queryset
from .registry import register

default_app_config = 'seeker.apps.SeekerConfig'
