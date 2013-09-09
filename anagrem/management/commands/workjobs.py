from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils.importlib import import_module

import anagrem.models


class Command(BaseCommand):

    help = 'Start a Gearman worker to work jobtasks'

    def handle(self, *args, **options):
        # Load all apps' jobs so they're registered.
        for app_name in settings.INSTALLED_APPS:
            try:
                import_module('.jobs', app_name)
            except ImportError:
                pass

        # Do the work now that everything's loaded.
        anagrem.models.client.work()
