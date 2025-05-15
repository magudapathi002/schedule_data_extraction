import os
from datetime import timedelta

from celery import Celery
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'weather_data_process.settings')

app = Celery('weather_data_process')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.conf.beat_schedule = {
    'run-shared-task-every-minute': {
        'task': 'forecast_process.tasks.download_all_file_grib_file',
        'schedule': crontab(hour=1),  # runs every minute
        # 'schedule': timedelta(seconds=1),  # runs every
    },
}


@app.task()
def add_numbers():
    return


app.autodiscover_tasks()
