from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

import anagrem.models


class Command(BaseCommand):

    help = 'Start a Gearman worker to work jobtasks'

    def handle(self, *args, **options):
        anagrem.models.client.work()
