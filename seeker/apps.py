from django.apps import AppConfig, apps
from django.core.exceptions import ImproperlyConfigured
from elasticsearch_dsl.connections import connections
import importlib
import inspect
import logging

logger = logging.getLogger(__name__)

class SeekerConfig (AppConfig):
    name = 'seeker'

    def ready(self):
        connections.create_connection()
        for app in apps.get_app_configs():
            try:
                mod = importlib.import_module(app.name + '.mappings')
            except ImportError:
                pass
            except:
                logger.exception('Error registering mapping')
