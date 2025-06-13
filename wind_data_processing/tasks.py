import os
from datetime import datetime

import pandas as pd
import requests
import numpy as np
import xarray as xr
from celery import shared_task
from django.db import transaction
from django.utils import timezone
from scipy.interpolate import PchipInterpolator
from .utils import get_or_create_today_status
from .models import GRIBCycleStatus, SunshineDuration, WindGust, Radiation, Precipitation, PrecipitationInterpolated, \
    RadiationInterpolated, TemperatureInterpolated, Temperature, CloudCoverInterpolated, TotalCloudCover, WindComponent, \
    InterpolatedVariable, Albedo, CAPE, Humidity

from .models import WindData10m, WindDataInterpolated
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pygrib


# @shared_task
# def download_cycle_grib_files(run_date=None, cycle="00", retry_count=0):
#     base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
#     run_date = run_date or datetime.utcnow().strftime('%Y%m%d')
#     save_dir = f"grib_data/{run_date}_{cycle}"
#     os.makedirs(save_dir, exist_ok=True)
#
#     forecast_hours = list(range(0, 121)) + list(range(123, 385, 3))
#     total_files = len(forecast_hours)
#
#     session = requests.Session()
#     retries = Retry(
#         total=5,
#         backoff_factor=1,
#         status_forcelist=[429, 500, 502, 503, 504],
#         allowed_methods=["GET"]
#     )
#     session.mount("https://", HTTPAdapter(max_retries=retries))
#
#     missing_files = []
#
#     for fhr in forecast_hours:
#         fhr_str = f"f{fhr:03d}"
#         filename = f"gfs.t{cycle}z.pgrb2.0p25.{fhr_str}"
#         out_path = os.path.join(save_dir, filename)
#
#         if os.path.exists(out_path):
#             print(f"✅ Already exists: {filename}")
#             continue
#
#         params = {
#             "file": filename,
#             "lev_10_m_above_ground": "on",
#             "var_UGRD": "on",
#             "var_VGRD": "on",
#             "subregion":'',
#             "toplat": 11.52,
#             "leftlon": 76.39,
#             "rightlon": 77.29,
#             "bottomlat": 10.37,
#             "dir": f"/gfs.{run_date}/{cycle}/atmos"
#         }
#
#         print(f"⬇️ Downloading {filename}...")
#         try:
#             response = session.get(base_url, params=params, stream=True, timeout=60)
#             if response.status_code == 200:
#                 with open(out_path, "wb") as f:
#                     for chunk in response.iter_content(chunk_size=8192):
#                         f.write(chunk)
#                 print(f"✅ Saved: {filename}")
#             elif response.status_code == 404:
#                 print(f"❌ 404 Not Found: {filename}")
#                 if os.path.exists(out_path):
#                     os.remove(out_path)
#                 missing_files.append(fhr)
#             else:
#                 print(f"❌ HTTP {response.status_code} for {filename}")
#                 missing_files.append(fhr)
#
#         except Exception as e:
#             print(f"⚠️ Error downloading {filename}: {e}")
#             missing_files.append(fhr)
#
#     if missing_files:
#         if retry_count < 10:
#             print(f"🔁 Missing {len(missing_files)} files. Retrying in 30 minutes...")
#             # Requeue the task manually using `.apply_async()`
#             download_cycle_grib_files.apply_async(
#                 kwargs={"run_date": run_date, "cycle": cycle, "retry_count": retry_count + 1},
#                 countdown=30 * 60
#             )
#         else:
#             print("❌ Max retries reached. Skipping this cycle.")
#         return
#
#     print(f"✅ All {total_files} files downloaded for cycle {cycle}")
#     extract_and_store_wind_data.delay(run_date=run_date, cycle=cycle)

@shared_task
def download_cycle_grib_files(run_date=None, cycle=None, retry_count=0):
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    run_date = run_date or datetime.utcnow().strftime('%Y%m%d')
    today_status = get_or_create_today_status()

    # Automatically decide which cycle to run if not provided
    if not cycle:
        cycle = today_status.get_next_pending_cycle()
        if not cycle:
            print(f"🎉 All cycles completed for {run_date}")
            return

    save_dir = f"grib_data/{run_date}_{cycle}"
    os.makedirs(save_dir, exist_ok=True)

    forecast_hours = list(range(0, 121)) + list(range(123, 385, 3))
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
            "lev_2_m_above_ground": "on",
            "lev_80_m_above_ground": "on",
            "lev_100_m_above_ground": "on",
            "lev_850_mb": "on",
            "lev_800_mb": "on",
            "lev_600_mb": "on",
            "lev_surface": "on",
            "var_ACPCP": "on",
            "var_ALBDO": "on",
            "var_CAPE": "on",
            "var_CPRAT": "on",
            "var_DLWRF": "on",
            "var_DSWRF": "on",
            "var_GUST": "on",
            "var_PRATE": "on",
            "var_RH": "on",
            "var_SUNSD": "on",
            "var_TCDC": "on",
            "var_TMAX": "on",
            "var_TMIN": "on",
            "var_TMP": "on",
            "var_UGRD": "on",
            "var_VGRD": "on",
            "subregion": '',
            "toplat": 11.52,
            "leftlon": 76.39,
            "rightlon": 77.29,
            "bottomlat": 10.37,
            "dir": f"/gfs.{run_date}/{cycle}/atmos"
        }

        print(f"⬇️ Downloading {filename}...")
        try:
            response = session.get(base_url, params=params, stream=True, timeout=60)
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
    files = sorted(f for f in os.listdir(data_dir) if f.endswith(".grib2") or f.endswith(".grb2") or "pgrb2" in f)

    for file in files:
        file_path = os.path.join(data_dir, file)
        try:
            grbs = pygrib.open(file_path)

            # --- Wind Components ---
            wind_levels = [
                ("10 metre U wind component", "heightAboveGround", 10),
                ("10 metre V wind component", "heightAboveGround", 10),
                ("U component of wind", "heightAboveGround", 80),
                ("V component of wind", "heightAboveGround", 80),
                ("100 metre U wind component", "heightAboveGround", 100),
                ("100 metre V wind component", "heightAboveGround", 100),
                ("U component of wind", "isobaricInhPa", 850),
                ("V component of wind", "isobaricInhPa", 850),
                ("U component of wind", "isobaricInhPa", 600),
                ("V component of wind", "isobaricInhPa", 600),
            ]
            for level_name, level_type, level_val in wind_levels:
                try:
                    u = grbs.select(name=level_name, typeOfLevel=level_type, level=level_val)[0]
                    v = grbs.select(name=level_name, typeOfLevel=level_type, level=level_val)[0]
                    u_data, lats, lons = u.data()
                    v_data, _, _ = v.data()
                    valid_time = u.validDate

                    with transaction.atomic():
                        for i in range(lats.shape[0]):
                            for j in range(lats.shape[1]):
                                WindComponent.objects.update_or_create(
                                    valid_time=valid_time,
                                    latitude=float(lats[i, j]),
                                    longitude=float(lons[i, j]),
                                    level=level_val,
                                    defaults={
                                        "u_value": float(u_data[i, j]),
                                        "v_value": float(v_data[i, j]),
                                    }
                                )
                except Exception as e:
                    print(f"❌ Error storing wind at {level_name}: {e}")

            # --- Temperature ---
            temperature_levels = [
                ("Temperature", "surface", 0, "t_surface"),
                ("2 metre temperature", "heightAboveGround", 2, "t2m"),
                ("Maximum temperature", "heightAboveGround", 2, "tmax"),
                ("Minimum temperature", "heightAboveGround", 2, "tmin"),
            ]
            for level_name, level_type, level_val, shortName in temperature_levels:
                try:
                    grb = grbs.select(name=level_name, typeOfLevel=level_type, level=level_val)[0]
                    data, lats, lons = grb.data()
                    valid_time = grb.validDate

                    with transaction.atomic():
                        for i in range(lats.shape[0]):
                            for j in range(lats.shape[1]):
                                Temperature.objects.update_or_create(
                                    valid_time=valid_time,
                                    latitude=float(lats[i, j]),
                                    longitude=float(lons[i, j]),
                                    short_name=shortName,
                                    level=level_val,
                                    defaults={"value": float(data[i, j])}
                                )
                except Exception as e:
                    print(f"⚠️ Skipping temperature {level_name}: {e}")

            humidity_levels = [
                ("2 metre relative humidity", "heightAboveGround", 2, "r2"),
            ]

            for level_name, level_type, level_val, ssname in humidity_levels:
                try:
                    grb = grbs.select(name=level_name, typeOfLevel=level_type, level=level_val)[0]
                    data, lats, lons = grb.data()
                    valid_time = grb.validDate

                    with transaction.atomic():
                        for i in range(lats.shape[0]):
                            for j in range(lats.shape[1]):
                                Humidity.objects.update_or_create(
                                    valid_time=valid_time,
                                    latitude=float(lats[i, j]),
                                    longitude=float(lons[i, j]),
                                    short_name=ssname,
                                    level=level_val,
                                    defaults={"value": float(data[i, j])}
                                )
                except Exception as e:
                    print(f"⚠️ Skipping humidity {level_name}: {e}")

            # --- Cloud Cover ---
            cloud_cover_levels = [
                ("Total Cloud Cover", "isobaricInhPa", 850, "tcc850"),
                ("Total Cloud Cover", "isobaricInhPa", 800, "tcc800"),
            ]

            for level_name, level_type, level_val, sssname in cloud_cover_levels:
                try:
                    grb = grbs.select(name=level_name, typeOfLevel=level_type, level=level_val)[0]
                    data, lats, lons = grb.data()
                    valid_time = grb.validDate

                    with transaction.atomic():
                        for i in range(lats.shape[0]):
                            for j in range(lats.shape[1]):
                                TotalCloudCover.objects.update_or_create(
                                    valid_time=valid_time,
                                    latitude=float(lats[i, j]),
                                    longitude=float(lons[i, j]),
                                    short_name=sssname,
                                    level=level_val,
                                    defaults={"value": float(data[i, j])}
                                )
                except Exception as e:
                    print(f"⚠️ Skipping cloud cover {level_val}mb: {e}")

            precipitation_types = [
                ("Convective precipitation (water)", "surface", 0, "acpcp"),
                ("Convective precipitation rate", "surface", 0, "cprat"),
                ("Precipitation rate", "surface", 0, "prate"),
            ]
            for level_name, level_type, level_val, ssssname in precipitation_types:
                try:
                    grb = grbs.select(name=level_name, typeOfLevel=level_type, level=level_val)[0]
                    data, lats, lons = grb.data()
                    valid_time = grb.validDate

                    with transaction.atomic():
                        for i in range(lats.shape[0]):
                            for j in range(lats.shape[1]):
                                Precipitation.objects.update_or_create(
                                    valid_time=valid_time,
                                    latitude=float(lats[i, j]),
                                    longitude=float(lons[i, j]),
                                    short_name=ssssname,
                                    level=level_val,
                                    defaults={"value": float(data[i, j])}
                                )
                except Exception as e:
                    print(f"⚠️ Skipping precipitation {level_type} at {level_val}: {e}")

            # --- Radiation ---
            for level_name, level_type, level_val, sname in [
                ("Downward long-wave radiation flux", "surface", 0, "dlwrf"),
                ("Downward short-wave radiation flux", "surface", 0, "dswrf"),
            ]:
                try:
                    grb = grbs.select(name=level_name, typeOfLevel=level_type, level=level_val)[0]
                    data, lats, lons = grb.data()
                    valid_time = grb.validDate

                    with transaction.atomic():
                        for i in range(lats.shape[0]):
                            for j in range(lats.shape[1]):
                                Radiation.objects.update_or_create(
                                    valid_time=valid_time,
                                    latitude=float(lats[i, j]),
                                    longitude=float(lons[i, j]),
                                    short_name=sname,
                                    level=level_val,
                                    defaults={"value": float(data[i, j])}
                                )
                except Exception as e:
                    print(f"⚠️ Skipping radiation {level_name}: {e}")

            # --- Wind Gust ---
            try:
                gust = grbs.select(name="Wind speed (gust)", typeOfLevel="surface", level=0)[0]
                gust_data, lats, lons = gust.data()
                valid_time = gust.validDate

                with transaction.atomic():
                    for i in range(lats.shape[0]):
                        for j in range(lats.shape[1]):
                            WindGust.objects.update_or_create(
                                valid_time=valid_time,
                                latitude=float(lats[i, j]),
                                longitude=float(lons[i, j]),
                                defaults={"value": float(gust_data[i, j])}
                            )
            except Exception as e:
                print(f"⚠️ Skipping wind gust: {e}")

            # --- Sunshine Duration ---
            try:
                sun = grbs.select(name="Sunshine duration", typeOfLevel="surface", level=0)[0]
                sun_data, lats, lons = sun.data()
                valid_time = sun.validDate

                with transaction.atomic():
                    for i in range(lats.shape[0]):
                        for j in range(lats.shape[1]):
                            SunshineDuration.objects.update_or_create(
                                valid_time=valid_time,
                                latitude=float(lats[i, j]),
                                longitude=float(lons[i, j]),
                                defaults={"value": float(sun_data[i, j])}
                            )
            except Exception as e:
                print(f"⚠️ Skipping sunshine duration: {e}")

            # --- CAPE ---
            try:
                cape = grbs.select(name="Convective available potential energy", typeOfLevel="surface", level=0)[0]
                data, lats, lons = cape.data()
                valid_time = cape.validDate

                with transaction.atomic():
                    for i in range(lats.shape[0]):
                        for j in range(lats.shape[1]):
                            CAPE.objects.update_or_create(
                                valid_time=valid_time,
                                latitude=float(lats[i, j]),
                                longitude=float(lons[i, j]),
                                defaults={"value": float(data[i, j])}
                            )
            except Exception as e:
                print(f"⚠️ Skipping CAPE: {e}")

            # --- Albedo ---
            try:
                alb = grbs.select(name="Forecast albedo", typeOfLevel="surface", level=0)[0]
                data, lats, lons = alb.data()
                valid_time = alb.validDate

                with transaction.atomic():
                    for i in range(lats.shape[0]):
                        for j in range(lats.shape[1]):
                            Albedo.objects.update_or_create(
                                valid_time=valid_time,
                                latitude=float(lats[i, j]),
                                longitude=float(lons[i, j]),
                                defaults={"value": float(data[i, j])}
                            )
            except Exception as e:
                print(f"⚠️ Skipping albedo: {e}")

            print(f"✅ Parsed and stored: {file}")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

    # interpolate_and_store_all_15min.delay()


# @shared_task
# def interpolate_and_store_wind_data_15min():
#     # Get all unique lat/lon pairs from raw data
#     locations = WindData10m.objects.values('latitude', 'longitude').distinct()
#
#     for loc in locations:
#         lat = loc['latitude']
#         lon = loc['longitude']
#
#         # Fetch all wind data for this lat/lon ordered by time
#         qs = WindData10m.objects.filter(latitude=lat, longitude=lon).order_by('valid_time')
#
#         if qs.count() < 2:
#             # Not enough data points to interpolate
#             continue
#
#         # Convert to DataFrame
#         df = pd.DataFrame.from_records(qs.values('valid_time', 'wind_speed'))
#         df['valid_time'] = pd.to_datetime(df['valid_time'])
#         df.set_index('valid_time', inplace=True)
#
#         # Create new time index with 15-minute frequency
#         new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15T')
#         df_15min = df.reindex(new_time_index)
#
#         # Interpolate missing values linearly
#         df_15min['wind_speed'] = df_15min['wind_speed'].interpolate(method='time')
#
#         # Prepare list for bulk insert
#         interpolated_entries = []
#         for time_val, row in df_15min.iterrows():
#             interpolated_entries.append(WindDataInterpolated(
#                 valid_time=time_val.to_pydatetime(),
#                 latitude=lat,
#                 longitude=lon,
#                 wind_speed=row['wind_speed']
#             ))
#
#         # Bulk insert into DB (you can delete previous interpolated entries if you want to overwrite)
#         WindDataInterpolated.objects.filter(latitude=lat, longitude=lon).delete()
#         WindDataInterpolated.objects.bulk_create(interpolated_entries)
#
#         print(f"Interpolated and saved data for lat: {lat}, lon: {lon} ({len(interpolated_entries)} records)")
#
#     print("✅ All locations processed.")

# Optional: use this helper for batching
@shared_task
def interpolate_and_store_all_15min(cycle=None, run_date=None):
    from django.db.models import Q

    variable_map = {
        'value': (Precipitation, PrecipitationInterpolated),
        'value': (Radiation, RadiationInterpolated),
        'value': (Temperature, TemperatureInterpolated),
        'value': (TotalCloudCover, CloudCoverInterpolated),
    }

    # ----- Scalar field interpolation -----
    for field, (raw_model, interp_model) in variable_map.items():
        print(f"📌 Interpolating: {raw_model.__name__}")

        locations = raw_model.objects.values_list('latitude', 'longitude').distinct()

        for lat, lon in locations:
            qs = raw_model.objects.filter(latitude=lat, longitude=lon).only("valid_time", field).order_by(
                "valid_time").iterator()
            data = [(obj.valid_time, getattr(obj, field)) for obj in qs if getattr(obj, field) is not None]

            if len(data) < 4:
                continue

            df = pd.DataFrame(data, columns=['valid_time', field])
            df['valid_time'] = pd.to_datetime(df['valid_time'])
            df.set_index('valid_time', inplace=True)
            df[field] = df[field].rolling(window=3, min_periods=1, center=True).mean()

            new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15min')

            try:
                interp = PchipInterpolator(df.index.astype(int) / 1e9, df[field])
                interpolated_values = interp(new_time_index.astype(int) / 1e9)
            except Exception as e:
                print(f"❌ PCHIP error at lat={lat}, lon={lon} for {raw_model.__name__}: {e}")
                continue

            updated_count = 0
            with transaction.atomic():
                for ts, val in zip(new_time_index, interpolated_values):
                    if pd.isnull(val):
                        continue
                    interp_model.objects.update_or_create(
                        valid_time=ts.to_pydatetime(),
                        latitude=lat,
                        longitude=lon,
                        defaults={field: round(float(val), 2)}
                    )
                    updated_count += 1
            print(f"✅ {raw_model.__name__} interpolated: lat={lat}, lon={lon} ({updated_count} records)")

    # ----- Multi-level Wind Speed Interpolation -----
    print(f"📌 Interpolating: wind_speed (multi-level)")
    levels = WindComponent.objects.values_list('level', flat=True).distinct()

    for level in levels:
        locations = WindComponent.objects.filter(level=level).values_list('latitude', 'longitude').distinct()

        for lat, lon in locations:
            qs = WindComponent.objects.filter(latitude=lat, longitude=lon, level=level).only("valid_time", "u_value",
                                                                                             "v_value").order_by(
                "valid_time").iterator()
            data = [
                (obj.valid_time, np.sqrt(obj.u_value ** 2 + obj.v_value ** 2))
                for obj in qs if obj.u_value is not None and obj.v_value is not None
            ]

            if len(data) < 4:
                continue

            df = pd.DataFrame(data, columns=['valid_time', 'wind_speed'])
            df['valid_time'] = pd.to_datetime(df['valid_time'])
            df.set_index('valid_time', inplace=True)
            df['wind_speed'] = df['wind_speed'].rolling(window=3, min_periods=1, center=True).mean()

            new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15min')

            try:
                interp = PchipInterpolator(df.index.astype(int) / 1e9, df['wind_speed'])
                interpolated_values = interp(new_time_index.astype(int) / 1e9)
            except Exception as e:
                print(f"❌ PCHIP error at lat={lat}, lon={lon} for wind_speed ({level}): {e}")
                continue

            updated_count = 0
            with transaction.atomic():
                for ts, val in zip(new_time_index, interpolated_values):
                    if pd.isnull(val):
                        continue
                    InterpolatedVariable.objects.update_or_create(
                        valid_time=ts.to_pydatetime(),
                        latitude=lat,
                        longitude=lon,
                        level=level,
                        variable='wind_speed',
                        defaults={"value": round(float(val), 2)}
                    )
                    updated_count += 1
            print(f"✅ wind_speed interpolated: lat={lat}, lon={lon}, level={level} ({updated_count} records)")

    # Cleanup .idx files if provided
    if run_date and cycle:
        delete_idx_files_for_cycle(run_date, cycle)

    print("🎉 All variables and levels processed successfully.")


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

# @shared_task
# def interpolate_and_store_wind_data_15min():
#     # Get all unique lat/lon pairs
#     locations = WindData10m.objects.values_list('latitude', 'longitude').distinct()
#
#     for lat, lon in locations:
#         # Efficient queryset and streaming
#         qs = WindData10m.objects.filter(latitude=lat, longitude=lon).only("valid_time", "wind_speed").order_by(
#             "valid_time").iterator()
#
#         data = [(obj.valid_time, obj.wind_speed) for obj in qs]
#         if len(data) < 4:
#             continue  # PCHIP needs at least 4 points
#
#         # Convert to DataFrame
#         df = pd.DataFrame(data, columns=['valid_time', 'wind_speed'])
#         df['valid_time'] = pd.to_datetime(df['valid_time'])
#         df.set_index('valid_time', inplace=True)
#
#         # Optional: apply smoothing
#         df['wind_speed'] = df['wind_speed'].rolling(window=3, min_periods=1, center=True).mean()
#
#         # Create new 15-min index
#         new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15T')
#
#         # Use PCHIP interpolation directly
#         interp = PchipInterpolator(df.index.astype(int) / 1e9, df['wind_speed'])  # seconds since epoch
#         interpolated_values = interp(new_time_index.astype(int) / 1e9)
#
#         # Prepare bulk data
#         interpolated_entries = [
#             WindDataInterpolated(
#                 valid_time=ts.to_pydatetime(),
#                 latitude=lat,
#                 longitude=lon,
#                 wind_speed=round(float(ws), 2)
#             )
#             for ts, ws in zip(new_time_index, interpolated_values)
#             if not pd.isnull(ws)
#         ]
#
#         # Remove old entries and insert new ones in chunks
#         WindDataInterpolated.objects.filter(latitude=lat, longitude=lon).delete()
#         for chunk in chunked(interpolated_entries, 1000):
#             with transaction.atomic():
#                 WindDataInterpolated.objects.bulk_create(chunk, batch_size=1000)
#
#         print(f"✅ Interpolated and stored data for lat={lat}, lon={lon} ({len(interpolated_entries)} records)")
#
#     print("🎉 All locations processed successfully.")


# @shared_task
# def spatially_interpolate_wind_data():
#     print("🚀 Starting spatial interpolation...")
#
#     # Get all unique timestamps from interpolated data
#     timestamps = WindDataInterpolated.objects.values_list('valid_time', flat=True).distinct()
#
#     total_inserted = 0
#
#     for ts in timestamps:
#         # Query all data at this timestamp
#         qs = WindDataInterpolated.objects.filter(valid_time=ts).only('latitude', 'longitude', 'wind_speed')
#
#         if qs.count() < 3:
#             print(f"⚠️ Skipping timestamp {ts} due to insufficient points (<3)")
#             continue  # griddata needs at least 3 points for linear interpolation
#
#         # Convert to DataFrame
#         df = pd.DataFrame.from_records(qs.values('latitude', 'longitude', 'wind_speed'))
#
#         points = df[['latitude', 'longitude']].values
#         values = df['wind_speed'].values
#
#         # Define spatial grid — adjust resolution as needed
#         lat_min, lat_max = df['latitude'].min(), df['latitude'].max()
#         lon_min, lon_max = df['longitude'].min(), df['longitude'].max()
#         lat_range = np.arange(lat_min, lat_max, 0.01)
#         lon_range = np.arange(lon_min, lon_max, 0.01)
#         grid_lat, grid_lon = np.meshgrid(lat_range, lon_range)
#
#         grid_points = np.c_[grid_lat.ravel(), grid_lon.ravel()]
#
#         try:
#             interpolated_values = griddata(points, values, grid_points, method='cubic')
#         except:
#             interpolated_values = griddata(points, values, grid_points, method='linear')  # fallback
#
#         # Fallback to nearest if linear returns too many NaNs
#         nan_ratio = np.isnan(interpolated_values).mean()
#         if nan_ratio > 0.3:  # Threshold: if more than 30% NaNs, fallback
#             print(f"⚠️ High NaN ratio ({nan_ratio:.2f}) at {ts}, falling back to nearest interpolation.")
#             interpolated_values = griddata(points, values, grid_points, method='nearest')
#
#         # Create model objects for valid points only
#         interpolated_entries = []
#         for (lat, lon), val in zip(grid_points, interpolated_values):
#             if np.isnan(val):
#                 continue
#             interpolated_entries.append(WindDataSpatialInterpolated(
#                 valid_time=ts,
#                 latitude=lat,
#                 longitude=lon,
#                 wind_speed=round(float(val), 2)
#             ))
#
#         # Insert in batches
#         for chunk in chunked(interpolated_entries, 1000):
#             with transaction.atomic():
#                 WindDataSpatialInterpolated.objects.bulk_create(chunk, batch_size=1000)
#
#         total_inserted += len(interpolated_entries)
#         print(f"✅ Spatial interpolation done for {ts} — inserted {len(interpolated_entries)} records")
#
#     print(f"🎉 Spatial interpolation complete! Total records inserted: {total_inserted}")
