import os

import pandas as pd
import requests
import numpy as np
import xarray as xr
from celery import shared_task
from .models import WindData10m,WindDataInterpolated
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@shared_task
def download_all_file_grib_file():
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    run_date = "20250515"
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
            "toplat": 13.09,
            "leftlon": 76.22,
            "rightlon": 80.4,
            "bottomlat": 8.02,
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

            wind_speed = np.sqrt(ugrd**2 + vgrd**2)
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

@shared_task
def interpolate_and_store_wind_data_15min():
    # Get all unique lat/lon pairs from raw data
    locations = WindData10m.objects.values('latitude', 'longitude').distinct()

    for loc in locations:
        lat = loc['latitude']
        lon = loc['longitude']

        # Fetch all wind data for this lat/lon ordered by time
        qs = WindData10m.objects.filter(latitude=lat, longitude=lon).order_by('valid_time')

        if qs.count() < 2:
            # Not enough data points to interpolate
            continue

        # Convert to DataFrame
        df = pd.DataFrame.from_records(qs.values('valid_time', 'wind_speed'))
        df['valid_time'] = pd.to_datetime(df['valid_time'])
        df.set_index('valid_time', inplace=True)

        # Create new time index with 15-minute frequency
        new_time_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='15T')
        df_15min = df.reindex(new_time_index)

        # Interpolate missing values linearly
        df_15min['wind_speed'] = df_15min['wind_speed'].interpolate(method='time')

        # Prepare list for bulk insert
        interpolated_entries = []
        for time_val, row in df_15min.iterrows():
            interpolated_entries.append(WindDataInterpolated(
                valid_time=time_val.to_pydatetime(),
                latitude=lat,
                longitude=lon,
                wind_speed=row['wind_speed']
            ))

        # Bulk insert into DB (you can delete previous interpolated entries if you want to overwrite)
        WindDataInterpolated.objects.filter(latitude=lat, longitude=lon).delete()
        WindDataInterpolated.objects.bulk_create(interpolated_entries)

        print(f"Interpolated and saved data for lat: {lat}, lon: {lon} ({len(interpolated_entries)} records)")

    print("✅ All locations processed.")