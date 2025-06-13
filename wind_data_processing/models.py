from django.db import models


# ---------- Raw Models Based on Provided Table ----------

class WindData10m(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    u_value = models.FloatField(null=True, default=None)
    v_value = models.FloatField(null=True, default=None)
    wind_speed = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude')


class WindComponent(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    level = models.CharField(max_length=50)  # e.g., 10m, 80m, 100m, 850mb, etc.
    u_value = models.FloatField()
    v_value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude', 'level')


class Precipitation(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    short_name = models.CharField(max_length=50,default=None)
    level = models.CharField(max_length=20)
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude', 'level','short_name')


class CAPE(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude')


class Albedo(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude')


class Radiation(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    short_name = models.CharField(max_length=50,default=None)
    level = models.CharField(max_length=10)  # DLWRF or DSWRF
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude', 'level','short_name')


class WindGust(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude')


class Humidity(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    short_name = models.CharField(max_length=50,default=None)
    level = models.CharField(max_length=10)  # 2m or 850mb
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude', 'level','short_name')


class SunshineDuration(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude')


class TotalCloudCover(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    short_name = models.CharField(max_length=50,default=None)
    level = models.CharField(max_length=10,default=None)  # e.g., 800mb, 850mb
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude', 'level','short_name')


class Temperature(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    short_name = models.CharField(max_length=50,default=None)
    level = models.CharField(max_length=10)  # e.g., 2m
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude', 'level','short_name')


# ---------- Interpolated 15-Minute Models ----------

class WindDataInterpolated(models.Model):
    valid_time = models.DateTimeField(db_index=True)
    latitude = models.FloatField(db_index=True)
    longitude = models.FloatField(db_index=True)
    level = models.CharField(max_length=50,default=None)  # e.g., 10m, 80m, 100m, 850mb, etc.
    u_value = models.FloatField(null=True, default=None)
    v_value = models.FloatField(null=True, default=None)
    wind_speed = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude')


class PrecipitationInterpolated(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    short_name = models.CharField(max_length=50, default=None)
    level = models.CharField(max_length=20,default=None)
    value = models.FloatField()


class RadiationInterpolated(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    short_name = models.CharField(max_length=50, default=None)
    level = models.CharField(max_length=20,default=None)
    value = models.FloatField()


class TemperatureInterpolated(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    short_name=models.CharField(max_length=50,default=None)
    level = models.CharField(max_length=20,default=None)
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude', 'level')


class CloudCoverInterpolated(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    short_name=models.CharField(max_length=50,default=None)
    level = models.CharField(max_length=20,default=None)
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude', 'level')


# ---------- GRIB Cycle Status Tracker ----------

class GRIBCycleStatus(models.Model):
    date = models.DateField()
    cycle_00 = models.CharField(max_length=20, default='pending')  # pending, completed, failed
    cycle_06 = models.CharField(max_length=20, default='pending')
    cycle_12 = models.CharField(max_length=20, default='pending')
    cycle_18 = models.CharField(max_length=20, default='pending')

    class Meta:
        unique_together = ('date',)
        ordering = ['-date']

    def get_next_pending_cycle(self):
        for cycle in ['00', '06', '12', '18']:
            if getattr(self, f'cycle_{cycle}') != 'completed':
                return cycle
        return None

class InterpolatedVariable(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    level = models.CharField(max_length=20,default=None)
    variable = models.CharField(max_length=50,default=None)  # e.g., wind_speed, wind_gust, etc.
    value = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude', 'level', 'variable')
