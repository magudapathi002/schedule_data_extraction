import os
import re
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
import requests
import numpy as np
from celery import shared_task
from django.db import transaction
from scipy.interpolate import PchipInterpolator

from .docs import TemperatureGribItem, HumidityGribItem, TotalCloudCoverGribItem, PrecipitationGribItem, \
    RadiationGribItem, WindGustGribItem, SunshineDurationGribItem, CAPE_GribItem, AlbedoGribItem, gfs_model_base_url, \
    forecast_hours, variable_map, generic_models, wind_component_pairs
from .utils import get_or_create_today_status, bulk_insert_and_update, process_scalar,round_half_up
from .models import SunshineDuration, WindGust, Radiation, Precipitation, Temperature, TotalCloudCover, WindComponent, \
    InterpolatedVariable, Albedo, CAPE, Humidity

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pygrib


@shared_task
def download_cycle_grib_files(run_date='20250701', cycle='18', retry_count=0):
    run_date = run_date or datetime.now(timezone.utc).strftime('%Y%m%d')
    today_status = get_or_create_today_status()

    # Automatically decide which cycle to run if not provided
    if not cycle:
        cycle = today_status.get_next_pending_cycle()
        if not cycle:
            print(f"🎉 All cycles completed for {run_date}")
            return

    save_dir = f"grib_data/{run_date}_{cycle}"
    os.makedirs(save_dir, exist_ok=True)
    total_files = len(forecast_hours)

    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    missing_files = []

    for fhr in forecast_hours:
        fhr_str = f"f{fhr:03d}"
        filename = f"gfs.t{cycle}z.pgrb2.0p25.{fhr_str}"
        out_path = os.path.join(save_dir, filename)

        if os.path.exists(out_path):
            print(f"✅ Already exists: {filename}")
            continue

        params = {
            "file": filename,
            "lev_10_m_above_ground": "on",
            # "lev_2_m_above_ground": "on",
            "lev_80_m_above_ground": "on",
            "lev_100_m_above_ground": "on",
            "lev_850_mb": "on",
            "lev_800_mb": "on",
            "lev_600_mb": "on",
            "lev_surface": "on",
            # "var_ACPCP": "on",
            # "var_ALBDO": "on",
            # "var_CAPE": "on",
            # "var_CPRAT": "on",
            # "var_DLWRF": "on",
            # "var_DSWRF": "on",
            # "var_GUST": "on",
            # "var_PRATE": "on",
            # "var_RH": "on",
            # "var_SUNSD": "on",
            # "var_TCDC": "on",
            # "var_TMAX": "on",
            # "var_TMIN": "on",
            # "var_TMP": "on",
            "var_UGRD": "on",
            "var_VGRD": "on",
            "subregion": '',
            "toplat": 13.58,
            "leftlon": 76.25,
            "rightlon": 80.33,
            "bottomlat": 8.05,
            "dir": f"/gfs.{run_date}/{cycle}/atmos"
        }

        print(f"⬇️ Downloading {filename}...")
        try:
            response = session.get(gfs_model_base_url, params=params, stream=True, timeout=60)
            if response.status_code == 200:
                with open(out_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"✅ Saved: {filename}")
            elif response.status_code == 404:
                print(f"❌ 404 Not Found: {filename}")
                if os.path.exists(out_path):
                    os.remove(out_path)
                missing_files.append(fhr)
            else:
                print(f"❌ HTTP {response.status_code} for {filename}")
                missing_files.append(fhr)
        except Exception as e:
            print(f"⚠️ Error downloading {filename}: {e}")
            missing_files.append(fhr)

    if missing_files:
        if retry_count < 5:
            print(f"🔁 Missing {len(missing_files)} files. Retrying in 30 minutes...")
            download_cycle_grib_files.apply_async(
                kwargs={"run_date": run_date, "cycle": cycle, "retry_count": retry_count + 1},
                countdown=30 * 60
            )
        else:
            print("❌ Max retries reached. Marking this cycle as failed.")
            setattr(today_status, f'cycle_{cycle}', 'failed')
            today_status.save()
        return

    print(f"✅ All {total_files} files downloaded for cycle {cycle}")
    extract_and_store_all_variables.delay(run_date=run_date, cycle=cycle)

    # Mark cycle as completed in the DB
    setattr(today_status, f'cycle_{cycle}', 'completed')
    today_status.save()
    print(f"📌 Updated DB status: {run_date} cycle {cycle} → completed")


@shared_task
def extract_and_store_all_variables(run_date, cycle):
    data_dir = f"grib_data/{run_date}_{str(cycle).zfill(2)}"
    files = sorted(f for f in os.listdir(data_dir) if re.match(r'.*\.f\d{3}$', f))

    for file in files:
        file_path = os.path.join(data_dir, file)
        forecast_hour = int(re.search(r'\.f(\d{3})$', file).group(1))

        try:
            grbs = pygrib.open(file_path)
            # --- ✅ Wind Components ---
            for u_name, v_name, level_type, level_val in wind_component_pairs:
                try:
                    # Safely select the first matching message
                    u_grb = next(iter(grbs.select(name=u_name, typeOfLevel=level_type, level=level_val)), None)
                    v_grb = next(iter(grbs.select(name=v_name, typeOfLevel=level_type, level=level_val)), None)

                    if not u_grb or not v_grb:
                        print(f"⚠️  Wind GRIBs not found at level={level_val}")
                        continue

                    u_data, lats, lons = u_grb.data()
                    v_data, _, _ = v_grb.data()
                    valid_time = u_grb.validDate

                    objects = []
                    for i in range(lats.shape[0]):
                        for j in range(lats.shape[1]):
                            u = u_data[i, j]
                            v = v_data[i, j]
                            speed = np.sqrt(u ** 2 + v ** 2)
                            direction = (np.degrees(np.arctan2(-u, -v)) + 360) % 360

                            obj = WindComponent(
                                valid_time=valid_time,
                                latitude=lats[i, j],
                                longitude=lons[i, j],
                                level=level_val,
                                u_value=round_half_up(u, 5),
                                v_value=round_half_up(v, 3),
                                wind_speed=round_half_up(speed, 3),
                                wind_direction=round_half_up(direction, 2)
                            )
                            objects.append(obj)
                    bulk_insert_and_update(
                        WindComponent,
                        objects,
                        ['valid_time', 'latitude', 'longitude', 'level'],
                        ['u_value', 'v_value', 'wind_speed', 'wind_direction']
                    )
                    print(f"✅ Wind data stored: level={level_val}, forecast_hour={forecast_hour}")
                except Exception as e:
                    print(f"❌ Error storing wind components at level={level_val} ({level_type}): {e}")
            # process_scalar(Temperature, TemperatureGribItem, short_name_field='short_name', grbs=grbs)
            # process_scalar(Humidity, HumidityGribItem, short_name_field='short_name', grbs=grbs)
            # process_scalar(TotalCloudCover, TotalCloudCoverGribItem, short_name_field='short_name', grbs=grbs)
            # process_scalar(Precipitation, PrecipitationGribItem, short_name_field='short_name', grbs=grbs)
            # process_scalar(Radiation, RadiationGribItem, short_name_field='short_name', grbs=grbs)
            # process_scalar(WindGust, WindGustGribItem, grbs=grbs)
            # process_scalar(SunshineDuration, SunshineDurationGribItem, grbs=grbs)
            # process_scalar(CAPE, CAPE_GribItem, grbs=grbs)
            # process_scalar(Albedo, AlbedoGribItem, grbs=grbs)
            print(f"✅ Parsed and stored: {file}")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

    interpolate_and_store_all_15min.delay()


@shared_task
def interpolate_and_store_all_15min(cycle=None, run_date=None):
    print(f"Interpolating: wind_speed and wind_direction (multi-level)")

    levels = WindComponent.objects.values_list('level', flat=True).distinct()

    for level in levels:
        data = WindComponent.objects.filter(level=level).values(
            'valid_time', 'latitude', 'longitude', 'wind_speed', 'wind_direction'
        )

        grouped = defaultdict(list)
        for entry in data:
            if entry['wind_speed'] is not None and entry['wind_direction'] is not None:
                key = (entry['latitude'], entry['longitude'])
                grouped[key].append((entry['valid_time'], entry['wind_speed'], entry['wind_direction']))

        for (lat, lon), time_series in grouped.items():
            if len(time_series) < 4:
                continue

            time_series.sort()
            df = pd.DataFrame(time_series, columns=['valid_time', 'wind_speed', 'wind_direction']).set_index('valid_time')

            if df.empty:
                continue

            # Create 15-min interpolation time index
            new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15min')

            try:
                interp_speed = PchipInterpolator(df.index.astype(np.int64) / 1e9, df['wind_speed'])
                interp_dir = PchipInterpolator(df.index.astype(np.int64) / 1e9, df['wind_direction'])

                interpolated_speeds = interp_speed(new_time_index.astype(np.int64) / 1e9)
                interpolated_directions = interp_dir(new_time_index.astype(np.int64) / 1e9)

            except Exception as e:
                print(f"❌ PCHIP error at lat={lat}, lon={lon} for level={level}: {e}")
                continue

            # Build objects to insert/update
            all_objs = []
            for ts, val_s, val_d in zip(new_time_index, interpolated_speeds, interpolated_directions):
                if pd.notnull(val_s):
                    all_objs.append(InterpolatedVariable(
                        valid_time=ts.to_pydatetime(),
                        latitude=lat,
                        longitude=lon,
                        level=level,
                        variable='wind_speed',
                        value=round(float(val_s), 2)
                    ))
                if pd.notnull(val_d):
                    all_objs.append(InterpolatedVariable(
                        valid_time=ts.to_pydatetime(),
                        latitude=lat,
                        longitude=lon,
                        level=level,
                        variable='wind_direction',
                        value=round(float(val_d), 2)
                    ))

            # Save to DB
            with transaction.atomic():
                # Insert new records; skip conflicts
                InterpolatedVariable.objects.bulk_create(all_objs, ignore_conflicts=True)

                # Update existing ones
                existing = InterpolatedVariable.objects.filter(
                    variable__in=['wind_speed', 'wind_direction'],
                    level=level,
                    latitude=lat,
                    longitude=lon,
                    valid_time__in=[obj.valid_time for obj in all_objs]
                )

                existing_map = {
                    (obj.valid_time, obj.latitude, obj.longitude, obj.level, obj.variable): obj
                    for obj in existing
                }

                updatable = []
                for obj in all_objs:
                    key = (obj.valid_time, obj.latitude, obj.longitude, obj.level, obj.variable)
                    if key in existing_map:
                        obj.pk = existing_map[key].pk  # Assign PK so Django knows to update
                        updatable.append(obj)

                if updatable:
                    InterpolatedVariable.objects.bulk_update(updatable, ['value'])

            print(f"✅ Interpolated and updated: lat={lat}, lon={lon}, level={level} ({len(all_objs)} records)")
    # for var_name, (field, raw_model, interp_model, extra_fields) in variable_map.items():
    #     print(f" Interpolating: {raw_model.__name__}")
    #     distinct_fields = ['latitude', 'longitude'] + extra_fields
    #     locations = raw_model.objects.values_list(*distinct_fields).distinct()
    #
    #     for loc in locations:
    #         lat, lon, *extras = loc
    #         filters = {'latitude': lat, 'longitude': lon}
    #         filters.update(dict(zip(extra_fields, extras)))
    #
    #         qs = raw_model.objects.filter(**filters)
    #         data = [(obj.valid_time, getattr(obj, field)) for obj in qs if getattr(obj, field) is not None]
    #         if len(data) < 4:
    #             continue
    #
    #         data.sort()
    #         df = pd.DataFrame(data, columns=['valid_time', field]).set_index('valid_time')
    #         df[field] = df[field].rolling(window=3, min_periods=1, center=True).mean()
    #         new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15min')
    #
    #         try:
    #             interp = PchipInterpolator(df.index.astype(np.int64) / 1e9, df[field])
    #             interpolated_values = interp(new_time_index.astype(np.int64) / 1e9)
    #         except Exception as e:
    #             print(f"❌ PCHIP error at {loc} for {raw_model.__name__}: {e}")
    #             continue
    #
    #         objects = [
    #             interp_model(
    #                 valid_time=ts.to_pydatetime(),
    #                 latitude=lat,
    #                 longitude=lon,
    #                 **dict(zip(extra_fields, extras)),
    #                 **{field: round(float(val), 2)}
    #             ) for ts, val in zip(new_time_index, interpolated_values) if not pd.isnull(val)
    #         ]
    #
    #         with transaction.atomic():
    #             interp_model.objects.bulk_create(objects, ignore_conflicts=True)
    #             existing = interp_model.objects.filter(
    #                 latitude=lat, longitude=lon,
    #                 valid_time__in=[obj.valid_time for obj in objects],
    #                 **dict(zip(extra_fields, extras))
    #             )
    #             existing_map = {obj.valid_time: obj for obj in existing}
    #             updatable = []
    #             for obj in objects:
    #                 if obj.valid_time in existing_map:
    #                     obj.pk = existing_map[obj.valid_time].pk
    #                     updatable.append(obj)
    #             interp_model.objects.bulk_update(updatable, [field])
    #
    #         print(f"✅ {raw_model.__name__} interpolated: {loc} ({len(objects)} records)")
    #
    # for model, variable_name, extra_fields in generic_models:
    #     print(f" Interpolating: {variable_name}")
    #     if extra_fields:
    #         distinct_fields = ['latitude', 'longitude'] + extra_fields
    #         locations = model.objects.values_list(*distinct_fields).distinct()
    #     else:
    #         locations = model.objects.values_list('latitude', 'longitude').distinct()
    #
    #     for loc in locations:
    #         lat, lon, *extras = loc
    #         filters = {'latitude': lat, 'longitude': lon}
    #         if extra_fields:
    #             filters.update(dict(zip(extra_fields, extras)))
    #
    #         qs = model.objects.filter(**filters)
    #         data = [(obj.valid_time, obj.value) for obj in qs if obj.value is not None]
    #         if len(data) < 4:
    #             continue
    #
    #         data.sort()
    #         df = pd.DataFrame(data, columns=['valid_time', 'value']).set_index('valid_time')
    #         df['value'] = df['value'].rolling(window=3, min_periods=1, center=True).mean()
    #         new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15min')
    #
    #         try:
    #             interp = PchipInterpolator(df.index.astype(np.int64) / 1e9, df['value'])
    #             interpolated_values = interp(new_time_index.astype(np.int64) / 1e9)
    #         except Exception as e:
    #             print(f"❌ PCHIP error at {loc} for {variable_name}: {e}")
    #             continue
    #
    #         objects = [
    #             InterpolatedVariable(
    #                 valid_time=ts.to_pydatetime(),
    #                 latitude=lat,
    #                 longitude=lon,
    #                 level=extras[0] if extra_fields and 'level' in extra_fields else None,
    #                 variable=variable_name,
    #                 value=round(float(val), 2)
    #             ) for ts, val in zip(new_time_index, interpolated_values) if not pd.isnull(val)
    #         ]
    #
    #         with transaction.atomic():
    #             InterpolatedVariable.objects.bulk_create(objects, ignore_conflicts=True)
    #             existing = InterpolatedVariable.objects.filter(
    #                 variable=variable_name, latitude=lat, longitude=lon,
    #                 valid_time__in=[obj.valid_time for obj in objects]
    #             )
    #             if extra_fields and 'level' in extra_fields:
    #                 existing = existing.filter(level=extras[0])
    #             existing_map = {
    #                 (obj.valid_time, obj.latitude, obj.longitude, obj.level, obj.variable): obj
    #                 for obj in existing
    #             }
    #             updatable = []
    #             for obj in objects:
    #                 key = (obj.valid_time, obj.latitude, obj.longitude, obj.level, obj.variable)
    #                 if key in existing_map:
    #                     obj.pk = existing_map[key].pk
    #                     updatable.append(obj)
    #             InterpolatedVariable.objects.bulk_update(updatable, ['value'])
    #
    #         print(f"✅ {variable_name} interpolated: {loc} ({len(objects)} records)")
    #
    print("✅ All scalar variable interpolations completed successfully.")
