from django.apps import AppConfig, apps
from elasticsearch_dsl.connections import connections
import importlib
import logging

logger = logging.getLogger(__name__)

class SeekerConfig (AppConfig):
    name = 'seeker'

    def ready(self):
        # TODO: create the connections based on settings
        connections.create_connection()
        for app in apps.get_app_configs():
            try:
                importlib.import_module(app.name + '.mappings')
            except ImportError:
                pass
