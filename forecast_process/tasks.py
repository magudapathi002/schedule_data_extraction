import os
import requests
import numpy as np
import xarray as xr
from celery import shared_task
from .models import WindData10m

@shared_task
def sharedtask():
    print("shared task")
    return 'hiiii'

@shared_task
def download_all_file_grib_file():
    base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    run_date = "20250514"
    cycle = "06"
    save_dir = "grib_data"
    os.makedirs(save_dir, exist_ok=True)

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
        response = requests.get(base_url, params=params, stream=True)
        if response.status_code == 200:
            out_path = os.path.join(save_dir, filename)
            with open(out_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Saved {filename}")
        else:
            print(f"Failed to download {filename}: HTTP {response.status_code}")
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
            ds = xr.open_dataset(path, engine="cfgrib", filter_by_keys={"typeOfLevel": "heightAboveGround", "level": 10})
            ugrd = ds['u10'] if 'u10' in ds else ds['u']
            vgrd = ds['v10'] if 'v10' in ds else ds['v']

            wind_speed = np.sqrt(ugrd**2 + vgrd**2)
            latitudes = ds.latitude.values
            longitudes = ds.longitude.values
            valid_time = ds.time.values[0] + ds.step.values[0]

            for lat_idx, lat in enumerate(latitudes):
                for lon_idx, lon in enumerate(longitudes):
                    WindData10m.objects.create(
                        valid_time=valid_time,
                        latitude=float(lat),
                        longitude=float(lon),
                        wind_speed=float(wind_speed[0, lat_idx, lon_idx])
                    )

            print(f"✅ Parsed and saved from: {file}")

        except Exception as e:
            print(f"⚠️ Error processing {file}: {e}")