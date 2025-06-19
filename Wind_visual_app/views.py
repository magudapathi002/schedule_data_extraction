from collections import defaultdict

from django.shortcuts import render
import json

from wind_data_processing.models import InterpolatedVariable


def wind_trend_view(request):
    lat, lon = 11.0, 77.25  # Example fixed location (can be dynamic)

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
        "levels": json.dumps(sorted(data_by_level.keys(), key=lambda x: float(x) if x.replace('.', '', 1).isdigit() else x)),
    }
    return render(request, "wind_trend/wind_trend.html", context)