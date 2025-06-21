from django.urls import path
from . import views
from .views import windspeed_query_view

urlpatterns = [
    path('', views.wind_trend_view, name='wind_trend'),
    path('wind-speed/', windspeed_query_view, name='wind_speed_query'),
]
