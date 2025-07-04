import os
from datetime import date

from django.db import transaction
from decimal import Decimal, ROUND_HALF_UP
from wind_data_processing.models import GRIBCycleStatus, WindComponent, Temperature, Humidity, TotalCloudCover, \
    Precipitation, Radiation, WindGust, SunshineDuration, CAPE, Albedo


def get_or_create_today_status():
    today = date.today()
    # today = '2025-07-01'
    status_obj, _ = GRIBCycleStatus.objects.get_or_create(date=today)
    return status_obj


def round_half_up(value, decimals):
    return str(Decimal(value).quantize(Decimal('1.' + '0' * decimals), rounding=ROUND_HALF_UP))



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

# --- Repeat for scalar fields ---
def process_scalar(model, grib_items, short_name_field=None,grbs = None):
    for name, level_type, level_val, short_name in grib_items:
        try:
            grb = grbs.select(name=name, typeOfLevel=level_type, level=level_val)[0]
            data, lats, lons = grb.data()
            valid_time = grb.validDate

            objects = []
            for i in range(lats.shape[0]):
                for j in range(lats.shape[1]):
                    obj = model(
                        valid_time=valid_time,
                        latitude=float(lats[i, j]),
                        longitude=float(lons[i, j]),
                        level=level_val,
                        **({short_name_field: short_name} if short_name_field else {}),
                        value=float(data[i, j])
                    )
                    objects.append(obj)

            unique_fields = ['valid_time', 'latitude', 'longitude']
            if short_name_field:
                unique_fields.append(short_name_field)
            if model != SunshineDuration and model != WindGust and model != CAPE and model != Albedo:
                unique_fields.append('level')

            bulk_insert_and_update(model, objects, unique_fields, ['value'])
        except Exception as e:
            print(f"⚠️ Skipping {model.__name__} {name}: {e}")

def bulk_update_existing(objects, existing, model, update_fields, key_fields):
    """
    Updates existing model instances in bulk based on a composite key.

    :param objects: New objects (e.g., queryset or list) to check and update.
    :param existing: Existing objects from the DB to match against.
    :param model: Django model class (e.g., InterpolatedVariable).
    :param update_fields: List of fields to update in bulk_update (e.g., ['value']).
    :param key_fields: Fields used to determine uniqueness (e.g., ['valid_time', 'latitude', 'longitude', 'level', 'variable']).
    """
    def get_key(obj):
        return tuple(getattr(obj, field) for field in key_fields)

    existing_map = {get_key(obj): obj for obj in existing}
    updatable = []

    for obj in objects:
        key = get_key(obj)
        if key in existing_map:
            obj.pk = existing_map[key].pk
            updatable.append(obj)

    model.objects.bulk_update(updatable, update_fields)
