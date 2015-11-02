from django.apps import AppConfig, apps
from django.core.exceptions import ImproperlyConfigured
from .mapping import Mapping
import importlib
import inspect
import logging

logger = logging.getLogger(__name__)

class SeekerConfig (AppConfig):
    name = 'seeker'

    def ready(self):
        self.mappings = []
        self.app_mappings = {}
        self.model_mappings = {}
        self.doc_types = {}
        loaded = set()
        for app in apps.get_app_configs():
            try:
                mod_name = app.name + '.mappings'
                mod = importlib.import_module(mod_name)
                for _name, item in inspect.getmembers(mod, inspect.isclass):
                    if item is not Mapping and issubclass(item, Mapping):
                        if item.__module__ in loaded:
                            # Ignore already-loaded mappings (i.e. in the case of subclassing or mappings imported by other mappings modules)
                            continue
                        mapping = item.instance()
                        if mapping.doc_type in self.doc_types:
                            raise ImproperlyConfigured('doc_type (%s) must be unique across all mappings [hint: check your mapping class names!]' % mapping.doc_type)
                        self.mappings.append(mapping)
                        self.app_mappings.setdefault(app.label, []).append(mapping)
                        self.doc_types[mapping.doc_type] = mapping
                        if mapping.model:
                            self.model_mappings.setdefault(mapping.model, []).append(mapping)
                loaded.add(mod_name)
            except ImportError:
                pass
            except:
                logger.exception('Error registering mapping')
