__version__ = '0.3.0'

from .mapping import Indexable, ModelIndex, document_from_model, document_field, RawString
from .facets import Facet, TermsFacet, YearHistogram
from .views import SeekerView, Column
from .utils import search, index
from .registry import register, documents, model_documents

default_app_config = 'seeker.apps.SeekerConfig'
