__version__ = '1.3.1'

from .mapping import *
from .query import *
from .views import SeekerView
from .utils import get_app_mappings, get_model_mappings, get_facet_filters, index, crossquery, queryset

default_app_config = 'seeker.apps.SeekerConfig'
