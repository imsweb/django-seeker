from .mapping import Indexable
from .registry import current_app, register
from .utils import import_class
from django.apps import AppConfig, apps
from django.conf import settings
import importlib
import inspect
import logging

logger = logging.getLogger(__name__)

class SeekerConfig (AppConfig):
    name = 'seeker'

    def ready(self):
        mappings = getattr(settings, 'SEEKER_MAPPINGS', [])
        if mappings:
            for mapping in mappings:
                mapping_cls = import_class(mapping)
                register(mapping_cls)
        else:
            mapping_module = getattr(settings, 'SEEKER_MAPPING_MODULE', 'mappings')
            if not mapping_module:
                return
            for app in apps.get_app_configs():
                current_app.label = app.label
                try:
                    module = '%s.%s' % (app.name, mapping_module)
                    imported_module = importlib.import_module(module)
                    clsmembers = inspect.getmembers(imported_module, lambda member: inspect.isclass(member) and member.__module__ == module and issubclass(member, Indexable))
                    for name, cls in clsmembers:
                        register(cls)
                except ImportError:
                    pass
            try:
                del current_app.label
            except:
                pass
