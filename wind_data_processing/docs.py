# Download Essentials
from wind_data_processing.models import Precipitation, PrecipitationInterpolated, Radiation, RadiationInterpolated, \
    Temperature, TemperatureInterpolated, TotalCloudCover, CloudCoverInterpolated, CAPE, Albedo, WindGust, Humidity, \
    SunshineDuration

gfs_model_base_url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
forecast_hours = list(range(0, 121)) + list(range(123, 385, 3))
# forecast_hours = list(range(0, 72))

# Variable Essentials
wind_component_pairs = [
    ("10 metre U wind component", "10 metre V wind component", "heightAboveGround", 10),
    ("U component of wind", "V component of wind", "heightAboveGround", 80),
    ("100 metre U wind component", "100 metre V wind component", "heightAboveGround", 100),
    ("U component of wind", "V component of wind", "isobaricInhPa", 850),
    ("U component of wind", "V component of wind", "isobaricInhPa", 600),
]

TemperatureGribItem = [
    ("Temperature", "surface", 0, "t_surface"),
    ("2 metre temperature", "heightAboveGround", 2, "t2m"),
    ("Maximum temperature", "heightAboveGround", 2, "tmax"),
    ("Minimum temperature", "heightAboveGround", 2, "tmin"),
]

HumidityGribItem = [("2 metre relative humidity", "heightAboveGround", 2, "r2")]

TotalCloudCoverGribItem = [
    ("Total Cloud Cover", "isobaricInhPa", 850, "tcc850"),
    ("Total Cloud Cover", "isobaricInhPa", 800, "tcc800")
]

PrecipitationGribItem = [
    ("Convective precipitation (water)", "surface", 0, "acpcp"),
    ("Convective precipitation rate", "surface", 0, "cprat"),
    ("Precipitation rate", "surface", 0, "prate"),
]

RadiationGribItem = [
    ("Downward long-wave radiation flux", "surface", 0, "dlwrf"),
    ("Downward short-wave radiation flux", "surface", 0, "dswrf"),
]

WindGustGribItem = [("Wind speed (gust)", "surface", 0, None)]

SunshineDurationGribItem = [("Sunshine Duration", "surface", 0, None)]

CAPE_GribItem = [("Convective available potential energy", "surface", 0, None)]

AlbedoGribItem = [("Forecast albedo", "surface", 0, None)]

# Interpolation Essentials
variable_map = {
        'precipitation': ('value', Precipitation, PrecipitationInterpolated, ['short_name', 'level']),
        'radiation': ('value', Radiation, RadiationInterpolated, ['short_name', 'level']),
        'temperature': ('value', Temperature, TemperatureInterpolated, ['short_name', 'level']),
        'cloud_cover': ('value', TotalCloudCover, CloudCoverInterpolated, ['level', 'short_name']),
    }

generic_models = [
    (CAPE, 'cape', ['level']),
    (Albedo, 'albedo',  ['level']),
    (WindGust, 'wind_gust',  ['level']),
    (Humidity, 'humidity', ['level', 'short_name']),
    (SunshineDuration, 'sunshine_duration',  ['level']),
]