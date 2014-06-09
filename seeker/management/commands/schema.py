from django.core.management.base import BaseCommand
from django.apps import apps
from seeker.utils import get_app_mappings
import pprint

class Command (BaseCommand):
    args = '<app1 app2 ...>'
    help = 'Re-indexes the specified applications'

    def handle(self, *args, **options):
        app_labels = args or [a.label for a in apps.get_app_configs()]
        for app_label in app_labels:
            print app_label
            print '=' * len(app_label)
            print
            for mapping in get_app_mappings(app_label):
                pprint.pprint(mapping.build_schema())
            print
