__version_info__ = (0, 1, 0)
__version__ = '.'.join(str(i) for i in __version_info__)

from .mapping import *
from .query import *
from .views import SeekerView
from .utils import get_app_mappings, get_model_mappings, get_facet_filters, index, crossquery

default_app_config = 'seeker.apps.SeekerConfig'
