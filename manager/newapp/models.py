from django.db import models

# Create your models here.
class WindData10m(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    wind_speed = models.FloatField()

    def __str__(self):
        return f"{self.valid_time} @ ({self.latitude}, {self.longitude}) - {self.wind_speed:.2f} m/s"