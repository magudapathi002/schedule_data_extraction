from django.db import models

# Create your models here.
class WindData10m(models.Model):
    valid_time = models.DateTimeField()
    latitude = models.FloatField()
    longitude = models.FloatField()
    u_value = models.FloatField(default=None, null=True)
    v_value = models.FloatField(default=None, null=True)
    wind_speed = models.FloatField()

    class Meta:
        unique_together = ('valid_time', 'latitude', 'longitude')
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