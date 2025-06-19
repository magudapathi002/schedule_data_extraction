import os
from datetime import date

from django.db import transaction

from wind_data_processing.models import GRIBCycleStatus, WindComponent, Temperature, Humidity, TotalCloudCover, \
    Precipitation, Radiation, WindGust, SunshineDuration, CAPE, Albedo


def url_generator(url):
    """
    A generator function that yields URLs from a given list of URLs.

    Args:
        url (list): A list of URLs.

    Yields:
        str: A URL from the input list.

    Raises:
        StopIteration: When all URLs have been yielded.
    """


def get_or_create_today_status():
    today = date.today()
    # today = '2025-05-30'
    status_obj, _ = GRIBCycleStatus.objects.get_or_create(date=today)
    return status_obj
def delete_idx_files_for_cycle(run_date, cycle):
    folder_path = f"grib_data/{run_date}_{cycle}"
    deleted_count = 0

    if not os.path.exists(folder_path):
        print(f"❌ Folder not found: {folder_path}")
        return

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".idx"):
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    deleted_count += 1
                except Exception as e:
                    print(f"⚠️ Failed to delete {file_path}: {e}")

    print(f"🧹 Deleted {deleted_count} .idx files from {folder_path}")

def bulk_insert_and_update(model, objects, unique_fields, update_fields):
    with transaction.atomic():
        model.objects.bulk_create(objects, ignore_conflicts=True)

        filter_kwargs = {
            field + '__in': list(set(getattr(obj, field) for obj in objects))
            for field in unique_fields if field != 'valid_time'
        }
        filter_kwargs['valid_time__in'] = [obj.valid_time for obj in objects]

        existing = model.objects.filter(**filter_kwargs)
        existing_map = {
            tuple(getattr(obj, f) for f in unique_fields): obj for obj in existing
        }

        updatable = []
        for obj in objects:
            key = tuple(getattr(obj, f) for f in unique_fields)
            if key in existing_map:
                obj.pk = existing_map[key].pk
                updatable.append(obj)

        if updatable:
            model.objects.bulk_update(updatable, update_fields)