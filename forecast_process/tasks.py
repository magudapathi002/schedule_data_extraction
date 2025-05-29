import os
from datetime import datetime

import pandas as pd
import requests
import numpy as np
import xarray as xr
from celery import shared_task
from django.db import transaction
from scipy.interpolate import PchipInterpolator
from .utils import get_or_create_today_status
from .models import GRIBCycleStatus

from .models import WindData10m, WindDataInterpolated, WindDataSpatialInterpolated
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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
        if retry_count < 10:
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
    extract_and_store_wind_data.delay(run_date=run_date, cycle=cycle)

    # Mark cycle as completed in the DB
    setattr(today_status, f'cycle_{cycle}', 'completed')
    today_status.save()
    print(f"📌 Updated DB status: {run_date} cycle {cycle} → completed")


@shared_task
def extract_and_store_wind_data(run_date, cycle):
    data_dir = f"grib_data/{run_date}_{cycle}"
    files = sorted(f for f in os.listdir(data_dir) if f.endswith(".grib2") or f.endswith(".grb2") or "pgrb2" in f)

    for file in files:
        path = os.path.join(data_dir, file)
        try:
            ds = xr.open_dataset(
                path,
                engine="cfgrib",
                filter_by_keys={"typeOfLevel": "heightAboveGround", "level": 10},
                backend_kwargs={"decode_timedelta": True}
            )

            # Retrieve wind components
            if 'u10' in ds and 'v10' in ds:
                ugrd = ds['u10']
                vgrd = ds['v10']
            elif 'u' in ds and 'v' in ds:
                ugrd = ds['u']
                vgrd = ds['v']
            else:
                raise KeyError("Missing wind components (u10/v10 or u/v)")

            wind_speed = np.sqrt(ugrd ** 2 + vgrd ** 2)
            latitudes = ds.latitude.values
            longitudes = ds.longitude.values

            # Safely extract valid_time
            time_val = ds.time.values
            if isinstance(time_val, np.ndarray):
                time_val = time_val[0]

            step_val = ds.step.values
            if isinstance(step_val, np.ndarray):
                step_val = step_val[0]

            # Combine to get final valid time
            valid_time = pd.to_datetime(time_val + step_val).to_pydatetime()

            shape = wind_speed.shape

            with transaction.atomic():
                for lat_idx, lat in enumerate(latitudes):
                    for lon_idx, lon in enumerate(longitudes):
                        if len(shape) == 3:
                            ws_value = round(float(wind_speed[0, lat_idx, lon_idx]), 2)
                        elif len(shape) == 2:
                            ws_value = round(float(wind_speed[lat_idx, lon_idx]), 2)
                        else:
                            raise ValueError(f"Unexpected wind_speed shape: {shape}")

                        WindData10m.objects.update_or_create(
                                valid_time=valid_time,
                                latitude=float(lat),
                                longitude=float(lon),
                            defaults={"wind_speed": ws_value}
                        )

            print(f"✅ Parsed and upserted: {file}")

        except Exception as e:
            print(f"⚠️ Error processing {file}: {e}")

    # Continue to the next task
    interpolate_and_store_wind_data_15min.delay()


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
def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

@shared_task
def interpolate_and_store_wind_data_15min():
    # Get all unique lat/lon pairs
    locations = WindData10m.objects.values_list('latitude', 'longitude').distinct()

    for lat, lon in locations:
        # Stream data efficiently
        qs = WindData10m.objects.filter(latitude=lat, longitude=lon).only("valid_time", "wind_speed").order_by("valid_time").iterator()

        data = [(obj.valid_time, obj.wind_speed) for obj in qs]
        if len(data) < 4:
            continue  # PCHIP needs at least 4 points

        # Prepare DataFrame
        df = pd.DataFrame(data, columns=['valid_time', 'wind_speed'])
        df['valid_time'] = pd.to_datetime(df['valid_time'])
        df.set_index('valid_time', inplace=True)

        # Smoothing (optional)
        df['wind_speed'] = df['wind_speed'].rolling(window=3, min_periods=1, center=True).mean()

        # Create 15-minute time index
        new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15min')

        # Interpolate using PCHIP
        interp = PchipInterpolator(df.index.astype(int) / 1e9, df['wind_speed'])
        interpolated_values = interp(new_time_index.astype(int) / 1e9)

        # Insert or update records
        updated_count = 0
        with transaction.atomic():
            for ts, ws in zip(new_time_index, interpolated_values):
                if pd.isnull(ws):
                    continue
                WindDataInterpolated.objects.update_or_create(
                    valid_time=ts.to_pydatetime(),
                    latitude=lat,
                    longitude=lon,
                    defaults={"wind_speed": round(float(ws), 2)}
                )
                updated_count += 1

        print(f"✅ Interpolated & upserted lat={lat}, lon={lon} ({updated_count} records)")

    print("🎉 All locations processed successfully.")

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
