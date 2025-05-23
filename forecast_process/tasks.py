import os

import pandas as pd
import requests
import numpy as np
import xarray as xr
from celery import shared_task
from django.db import transaction
from scipy.interpolate import griddata, PchipInterpolator

from .models import WindData10m, WindDataInterpolated, WindDataSpatialInterpolated
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@shared_task
def download_all_file_grib_file():
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    run_date = "20250521"
    cycle = "00"
    save_dir = "grib_data"
    os.makedirs(save_dir, exist_ok=True)

    # Setup retry session
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Forecast hours: 0-120 hourly + 123 to 384 every 3 hours
    forecast_hours = list(range(0, 121)) + list(range(123, 385, 3))

    for fhr in forecast_hours:
        fhr_str = f"f{fhr:03d}"
        filename = f"gfs.t{cycle}z.pgrb2.0p25.{fhr_str}"
        params = {
            "file": filename,
            "lev_10_m_above_ground": "on",
            "var_UGRD": "on",
            "var_VGRD": "on",
            "subregion": "",
            "toplat": 11.52,
            "leftlon": 76.39,
            "rightlon": 77.29,
            "bottomlat": 10.37,
            "dir": f"/gfs.{run_date}/{cycle}/atmos"
        }

        print(f"Downloading {filename}...")
        try:
            response = session.get(base_url, params=params, stream=True, timeout=60)
            if response.status_code == 200:
                out_path = os.path.join(save_dir, filename)
                with open(out_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"Saved {filename}")
            else:
                print(f"Failed to download {filename}: HTTP {response.status_code}")
        except Exception as e:
            print(f"⚠️ Error downloading {filename}: {e}")

    print('all files is downloaded')

    # ⏩ Now trigger the extraction task
    extract_and_store_wind_data.delay()

    return "Download + extraction triggered"


@shared_task
def extract_and_store_wind_data():
    data_dir = "grib_data"
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

            # Retrieve wind components (try u10/v10 first, then fallback to u/v)
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

            # Convert to Python datetime to avoid fromisoformat errors
            valid_time = pd.to_datetime(time_val + step_val).to_pydatetime()

            wind_data_entries = []
            shape = wind_speed.shape

            for lat_idx, lat in enumerate(latitudes):
                for lon_idx, lon in enumerate(longitudes):
                    # Handle both 2D and 3D arrays
                    if len(shape) == 3:
                        ws_value = round(float(wind_speed[0, lat_idx, lon_idx]), 2)
                    elif len(shape) == 2:
                        ws_value = round(float(wind_speed[lat_idx, lon_idx]), 2)
                    else:
                        raise ValueError(f"Unexpected wind_speed shape: {shape}")

                    wind_data_entries.append(WindData10m(
                        valid_time=valid_time,
                        latitude=float(lat),
                        longitude=float(lon),
                        wind_speed=ws_value
                    ))
            WindData10m.objects.bulk_create(wind_data_entries)
            print(f"✅ Parsed and saved from: {file} ({len(wind_data_entries)} records)")
        except Exception as e:
            print(f"⚠️ Error processing {file}: {e}")
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
        # Efficient queryset and streaming
        qs = WindData10m.objects.filter(latitude=lat, longitude=lon).only("valid_time", "wind_speed").order_by(
            "valid_time").iterator()

        data = [(obj.valid_time, obj.wind_speed) for obj in qs]
        if len(data) < 4:
            continue  # PCHIP needs at least 4 points

        # Convert to DataFrame
        df = pd.DataFrame(data, columns=['valid_time', 'wind_speed'])
        df['valid_time'] = pd.to_datetime(df['valid_time'])
        df.set_index('valid_time', inplace=True)

        # Optional: apply smoothing
        df['wind_speed'] = df['wind_speed'].rolling(window=3, min_periods=1, center=True).mean()

        # Create new 15-min index
        new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15T')

        # Use PCHIP interpolation directly
        interp = PchipInterpolator(df.index.astype(int) / 1e9, df['wind_speed'])  # seconds since epoch
        interpolated_values = interp(new_time_index.astype(int) / 1e9)

        # Prepare bulk data
        interpolated_entries = [
            WindDataInterpolated(
                valid_time=ts.to_pydatetime(),
                latitude=lat,
                longitude=lon,
                wind_speed=round(float(ws), 2)
            )
            for ts, ws in zip(new_time_index, interpolated_values)
            if not pd.isnull(ws)
        ]

        # Remove old entries and insert new ones in chunks
        WindDataInterpolated.objects.filter(latitude=lat, longitude=lon).delete()
        for chunk in chunked(interpolated_entries, 1000):
            with transaction.atomic():
                WindDataInterpolated.objects.bulk_create(chunk, batch_size=1000)

        print(f"✅ Interpolated and stored data for lat={lat}, lon={lon} ({len(interpolated_entries)} records)")

    print("🎉 All locations processed successfully.")
    # Optional: trigger next step
    from .tasks import spatially_interpolate_wind_data
    spatially_interpolate_wind_data.delay()


@shared_task
def spatially_interpolate_wind_data():
    print("🚀 Starting spatial interpolation...")

    # Get all unique timestamps from interpolated data
    timestamps = WindDataInterpolated.objects.values_list('valid_time', flat=True).distinct()

    total_inserted = 0

    for ts in timestamps:
        # Query all data at this timestamp
        qs = WindDataInterpolated.objects.filter(valid_time=ts).only('latitude', 'longitude', 'wind_speed')

        if qs.count() < 3:
            print(f"⚠️ Skipping timestamp {ts} due to insufficient points (<3)")
            continue  # griddata needs at least 3 points for linear interpolation

        # Convert to DataFrame
        df = pd.DataFrame.from_records(qs.values('latitude', 'longitude', 'wind_speed'))

        points = df[['latitude', 'longitude']].values
        values = df['wind_speed'].values

        # Define spatial grid — adjust resolution as needed
        lat_min, lat_max = df['latitude'].min(), df['latitude'].max()
        lon_min, lon_max = df['longitude'].min(), df['longitude'].max()
        lat_range = np.arange(lat_min, lat_max, 0.01)
        lon_range = np.arange(lon_min, lon_max, 0.01)
        grid_lat, grid_lon = np.meshgrid(lat_range, lon_range)

        grid_points = np.c_[grid_lat.ravel(), grid_lon.ravel()]

        # Try linear interpolation first
        interpolated_values = griddata(points, values, grid_points, method='linear')

        # Fallback to nearest if linear returns too many NaNs
        nan_ratio = np.isnan(interpolated_values).mean()
        if nan_ratio > 0.3:  # Threshold: if more than 30% NaNs, fallback
            print(f"⚠️ High NaN ratio ({nan_ratio:.2f}) at {ts}, falling back to nearest interpolation.")
            interpolated_values = griddata(points, values, grid_points, method='nearest')

        # Create model objects for valid points only
        interpolated_entries = []
        for (lat, lon), val in zip(grid_points, interpolated_values):
            if np.isnan(val):
                continue
            interpolated_entries.append(WindDataSpatialInterpolated(
                valid_time=ts,
                latitude=lat,
                longitude=lon,
                wind_speed=round(float(val), 2)
            ))

        # Insert in batches
        for chunk in chunked(interpolated_entries, 1000):
            with transaction.atomic():
                WindDataSpatialInterpolated.objects.bulk_create(chunk, batch_size=1000)

        total_inserted += len(interpolated_entries)
        print(f"✅ Spatial interpolation done for {ts} — inserted {len(interpolated_entries)} records")

    print(f"🎉 Spatial interpolation complete! Total records inserted: {total_inserted}")
