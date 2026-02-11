import json

from django.core.management.base import BaseCommand
from seeker.dsl import connections, AuthorizationException
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


class Command(BaseCommand):
    help = "Dumps out data from the specified document types"

    def add_arguments(self, parser):
        parser.add_argument(
            "--index",
            dest="index",
            default=None,
            help="Index to dump",
        )
        parser.add_argument(
            "--using",
            dest="using",
            default="default",
            help="The ES/OS connection alias to use",
        )

    def handle(self, *args, **options):
        connection = connections.get_connection(options["using"])
        idx = options["index"]
        prefix = getattr(settings, "SEEKER_INDEX_PREFIX", None)
        if not (idx or prefix):
            raise ImproperlyConfigured(
                "An index or index prefix must be supplied (either through --index or SEEKER_INDEX_PREFIX setting)"
            )
        idx = idx or f"{prefix}*"

        try:
            self.stdout.write(json.dumps(connection.indices.get(index=idx), indent=4))
        except AuthorizationException:
            self.stderr.write(f'You are not authorized to access index: "{idx}")')
