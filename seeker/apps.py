from .registry import current_app
from django.apps import AppConfig, apps
from django.conf import settings
import importlib
import logging

logger = logging.getLogger(__name__)

class SeekerConfig (AppConfig):
    name = 'seeker'

    def ready(self):
        mapping_module = getattr(settings, 'SEEKER_MAPPING_MODULE', 'mappings')
        if not mapping_module:
            return
        for app in apps.get_app_configs():
            current_app.label = app.label
            try:
                importlib.import_module('%s.%s' % (app.name, mapping_module))
            except ImportError:
                pass
        try:
            del current_app.label
        except:
            pass
