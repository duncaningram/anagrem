from django.conf import settings

import anagrem


client = anagrem.Client(settings.GEARMAN_SERVERS)

task = client.task


@task
def moose():
    return {'moose': True}


@task
def waitmoose():
    time.sleep(60)
    return {'moose': True}


@task
def failmoose():
    raise NotImplementedError()
