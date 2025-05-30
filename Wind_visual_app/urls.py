from django.urls import path
from . import views

urlpatterns = [
    path('', views.wind_trend_view, name='wind_trend'),
]
