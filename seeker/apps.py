import importlib
import inspect
import logging
logging.basicConfig()
from django.apps import AppConfig, apps
from django.conf import settings

from .mapping import Indexable
from .registry import register
from .utils import import_class


logger = logging.getLogger(__name__)


class SeekerConfig(AppConfig):
    name = 'seeker'

    def ready(self):
        mapping_module = getattr(settings, 'SEEKER_MAPPING_MODULE', 'mappings')
        mappings = getattr(settings, 'SEEKER_MAPPINGS', [])
        module_only = getattr(settings, 'SEEKER_MODULE_ONLY', True)
        if mappings:
            # Keep a mapping of app module to app label (project.app.subapp -> subapp)
            app_lkup = {app.name: app.label for app in apps.get_app_configs()}
            for mapping in mappings:
                mapping_cls = import_class(mapping)
                if not mapping_cls.model:
                    logger.warning('model not defined on %s You must define the model for a significant speed increase', mapping)
                    
                # Figure out which app_label to use based on the longest matching module prefix.
                app_label = None
                for prefix in sorted(app_lkup):
                    if mapping.startswith(prefix):
                        app_label = app_lkup[prefix]
                register(mapping_cls, app_label=app_label)
        else:
            if not mapping_module:
                return
            for app in apps.get_app_configs():
                try:
                    module = '%s.%s' % (app.name, mapping_module)
                    imported_module = importlib.import_module(module)
                    clsmembers = inspect.getmembers(imported_module, lambda member: inspect.isclass(member) and issubclass(member, Indexable))
                    for name, cls in clsmembers:
                        if not cls.model:
                            logger.warning('model not defined on %s.%s You must define the model for a significant speed increase', cls.__module__, name)
                        if module_only and cls.__module__ != module:
                            logger.debug('Skipping registration of %s.%s (defined outside %s)', cls.__module__, name, module)
                            continue
                        register(cls, app_label=app.label)
                except ImportError:
                    pass

        self.indexer = None
        indexer_module = getattr(settings, 'SEEKER_INDEXER', 'seeker.indexer.ModelIndexer')
        if indexer_module is not None:
            indexer_cls = None
            try:
                indexer_cls = import_class(indexer_module)
            except ImportError:
                logger.error("Error importing indexer '{}' specified in settings.SEEKER_INDEXER".format(indexer_module))
            if indexer_cls is not None:
                self.indexer = indexer_cls()
                self.indexer.connect_signal_handlers()
