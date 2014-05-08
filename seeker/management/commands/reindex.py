from django.core.management.base import BaseCommand
from django.apps import apps
from seeker.utils import get_app_mappings
from elasticsearch.helpers import bulk
import tqdm

class Command (BaseCommand):
    args = '<app1 app2 ...>'
    help = 'Re-indexes the specified applications'

    def handle(self, *args, **options):
        app_labels = args or [a.label for a in apps.get_app_configs()]
        for app_label in app_labels:
            for mapping in get_app_mappings(app_label):
                # If the index doesn't exist, create it.
                if not mapping.es.indices.exists(index=mapping.index_name):
                    mapping.es.indices.create(index=mapping.index_name)
                # If the mapping (type) already exists, delete it first.
                mapping.clear()
                # Create the new mapping.
                mapping.refresh()
                # Index all the things, using ElasticSearch's bulk API for speed.
                action = {
                    '_index': mapping.index_name,
                    '_type': mapping.doc_type,
                }
                def get_actions():
                    for obj in mapping.get_objects():
                        action.update({
                            '_id': mapping.get_id(obj),
                            '_source': mapping.get_data(obj),
                        })
                        yield action
                try:
                    total = mapping.queryset().count()
                except:
                    total = None
                print '%s.%s:' % (app_label, mapping.__class__.__name__)
                bulk(mapping.es, tqdm.tqdm(get_actions(), total=total, leave=True))
                print
