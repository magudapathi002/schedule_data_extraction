from django.db import models

# Create your models here.
class WindData10m(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    u_value = models.FloatField(default=None, null=True)
    v_value = models.FloatField(default=None, null=True)
    wind_speed = models.FloatField()

    def __str__(self):
        return f"{self.valid_time} @ ({self.latitude}, {self.longitude}) - {self.wind_speed:.2f} m/s"


class WindDataInterpolated(models.Model):
    valid_time = models.DateTimeField(db_index=True)
    latitude = models.FloatField(db_index=True)
    longitude = models.FloatField(db_index=True)
    wind_speed = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude')

# Example model (make sure you have this or similar model defined)
class WindDataSpatialInterpolated(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    wind_speed = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude')
