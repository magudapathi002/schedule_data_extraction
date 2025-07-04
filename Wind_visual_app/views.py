import math
import pprint
from collections import defaultdict
from io import BytesIO

import numpy as np
import pandas as pd
from celery.utils.serialization import jsonify
from django.db import models
from django.db.models import ExpressionWrapper, F, FloatField
from django.db.models.functions import Sqrt, Power
from django.http import HttpResponse
from django.shortcuts import render
import json

from scipy.spatial import KDTree

from Wind_visual_app.forms import WindSpeedQueryForm
from wind_data_processing.models import InterpolatedVariable


def wind_trend_view(request):
    lat, lon = 8.25, 77.25  # Example fixed location (can be dynamic)

    # Fetch all wind_speed data at this location
    records = (
        InterpolatedVariable.objects
        .filter(latitude=lat, longitude=lon, variable='wind_speed')
        .order_by('valid_time')
        .values('valid_time', 'value', 'level')
    )

    # Structure: {level: {day: {"times": [...], "speeds": [...]}}}
    data_by_level = defaultdict(lambda: defaultdict(lambda: {"times": [], "speeds": []}))

    for entry in records:
        level = entry["level"]
        valid_time = entry["valid_time"]
        day = valid_time.date().isoformat()
        time_str = valid_time.strftime("%H:%M")
        speed = float(entry["value"]) if entry["value"] is not None else 0.0
        data_by_level[level][day]["times"].append(time_str)
        data_by_level[level][day]["speeds"].append(round(speed, 2))

    context = {
        "location": f"{lat}, {lon}",
        "data_by_level": json.dumps(data_by_level),
        "levels": json.dumps(
            sorted(data_by_level.keys(), key=lambda x: float(x) if x.replace('.', '', 1).isdigit() else x)),
    }
    return render(request, "wind_trend/wind_trend.html", context)


from django.db import connection

from django.shortcuts import render
from django.http import HttpResponse
from django.db import connection
from io import BytesIO
import pandas as pd
import math
import pprint

def windspeed_query_view(request):
    form = WindSpeedQueryForm(request.POST or None)
    results = []
    nearest_locations = []

    if form.is_valid():
        lat_str = form.cleaned_data['latitudes']
        lon_str = form.cleaned_data['longitudes']
        level = form.cleaned_data['level']
        from_dt = form.cleaned_data['from_datetime']
        to_dt = form.cleaned_data['to_datetime']

        try:
            user_lat = float(lat_str.strip())
            user_lon = float(lon_str.strip())
        except ValueError:
            form.add_error(None, "Invalid latitude or longitude format. Please provide single numeric values.")
            return render(request, 'wind_query.html', {
                'form': form,
                'results': results,
                'nearest_locations': nearest_locations
            })

        # Step 1: Get all distinct lat/lon from windcomponent table for the given level
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT latitude, longitude
                FROM wind_data_processing_windcomponent
                WHERE level = %s
            """, [level])
            available_coords = cursor.fetchall()  # List of (lat, lon)

        # Step 2: Find nearest lat/lon
        def find_nearest(user_lat, user_lon, coords):
            return min(coords, key=lambda coord: math.hypot(coord[0] - user_lat, coord[1] - user_lon))

        nearest_lat, nearest_lon = find_nearest(user_lat, user_lon, available_coords)
        nearest_locations.append((user_lat, user_lon, nearest_lat, nearest_lon))

        # Step 3: Fetch wind data from interpolatedvariable
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id, valid_time, latitude, longitude, level, value
                FROM wind_data_processing_interpolatedvariable
                WHERE variable = 'wind_speed'
                  AND level = %s
                  AND latitude = %s
                  AND longitude = %s
                  AND valid_time BETWEEN %s AND %s
                ORDER BY valid_time
            """, [level, nearest_lat, nearest_lon, from_dt, to_dt])
            rows = cursor.fetchall()

            for row in rows:
                results.append({
                    'id': row[0],
                    'valid_time': row[1],
                    'latitude': row[2],
                    'longitude': row[3],
                    'level': row[4],
                    'value': row[5],
                })

        pprint.pprint(connection.queries)

        # Step 4: Handle download
        if request.POST.get("action") == "download":
            df = pd.DataFrame([{
                'user_lat': user_lat,
                'user_lon': user_lon,
                'nearest_lat': nearest_lat,
                'nearest_lon': nearest_lon,
                'valid_time': r['valid_time'],
                'level': r['level'],
                'value': r['value']
            } for r in results])

            buffer = BytesIO()
            df.to_excel(buffer, index=False)
            buffer.seek(0)
            response = HttpResponse(
                buffer,
                content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            response['Content-Disposition'] = 'attachment; filename=wind_speed_nearest_forecast.xlsx'
            return response

    return render(request, 'wind_query.html', {
        'form': form,
        'results': results,
        'nearest_locations': nearest_locations
    })

