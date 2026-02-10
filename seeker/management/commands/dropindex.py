from django.conf import settings
from django.core.management.base import BaseCommand
from seeker.dsl import AuthorizationException, connections
from django.core.exceptions import ImproperlyConfigured


class Command(BaseCommand):
    help = "Drops all ES/OS indexes on project with SEEKER_INDEX_PREFIX from settings, or one that you specify. To drop indexes with prefix add wildcard * after prefix of indexes you want deleted"

    def add_arguments(self, parser):
        parser.add_argument(
            "--index", dest="index", default=None, help="The ES/OS index(es) to drop"
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

        self.stdout.write(f"Using connection: {options['using']}")
        self.stdout.write(f"Attempting to drop index(es) using the pattern: {idx}")
        for index in connection.indices.get(index=idx):
            try:
                if connection.indices.exists(index=index):
                    connection.indices.delete(index=index)
                    if connection.indices.exists(index=index):
                        self.stdout.write(
                            f"{index} ...This index was NOT successfully dropped."
                        )
                    else:
                        self.stdout.write(f"{index} ... dropped.")
                else:
                    self.stdout.write(
                        f"{index} ... The index could not be dropped because it does not exist."
                    )
            except AuthorizationException:
                self.stderr.write(f'You are not authorized to drop index: "{index}")')
        self.stdout.write("Done. Please verify statements above for success/failure.")
