import os
from datetime import timedelta

from celery import Celery
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'manager.settings')

app = Celery('manager')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.conf.beat_schedule = {
    'run-shared-task-every-minute': {
        'task': 'newapp.tasks.sharedtask',
        # 'schedule': crontab(),  # runs every minute
        'schedule': timedelta(seconds=1),  # runs every
    },
}


@app.task()
def add_numbers():
    return


app.autodiscover_tasks()
