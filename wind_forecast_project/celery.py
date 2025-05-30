import os
from datetime import timedelta

from celery import Celery
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'wind_forecast_project.settings')

app = Celery('wind_forecast_project')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.conf.beat_schedule = {
    'download-grib-cycle-00': {
        'task': 'wind_data_processing.tasks.download_cycle_grib_files',
        'schedule': crontab(minute=40, hour=5),  # 05:40 UTC
        'kwargs': {"run_date": None, "cycle": "00"}
    },
    'download-grib-cycle-06': {
        'task': 'wind_data_processing.tasks.download_cycle_grib_files',
        'schedule': crontab(minute=40, hour=11),  # 11:40 UTC
        'kwargs': {"run_date": None, "cycle": "06"}
    },
    'download-grib-cycle-12': {
        'task': 'wind_data_processing.tasks.download_cycle_grib_files',
        'schedule': crontab(minute=40, hour=17),  # 17:40 UTC
        'kwargs': {"run_date": None, "cycle": "12"}
    },
    'download-grib-cycle-18': {
        'task': 'wind_data_processing.tasks.download_cycle_grib_files',
        'schedule': crontab(minute=40, hour=23),  # 23:40 UTC
        'kwargs': {"run_date": None, "cycle": "18"}
    },
}

app.autodiscover_tasks()