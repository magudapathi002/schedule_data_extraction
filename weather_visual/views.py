from collections import defaultdict

from django.shortcuts import render
import json

from forecast_process.models import WindData10m, WindDataInterpolated


def wind_trend_view(request):
    # Example fixed location (you can make it dynamic)

    # 11.024205, 77.006990
    lat, lon = 11, 77

    data = (
        WindDataInterpolated.objects
        .filter(latitude=lat, longitude=lon)
        .order_by("valid_time")
        .values("valid_time", "wind_speed")
    )

    # Group data by day
    daily_data = defaultdict(lambda: {"times": [], "speeds": []})
    for entry in data:
        day = entry["valid_time"].date().isoformat()
        time_str = entry["valid_time"].strftime("%H:%M")
        wind_speed = float(entry["wind_speed"]) if entry["wind_speed"] is not None else 0.0
        daily_data[day]["times"].append(time_str)
        daily_data[day]["speeds"].append(round(wind_speed, 2))

    context = {
        "days": json.dumps(list(daily_data.keys())),
        "data_by_day": json.dumps(daily_data),
        "location": f"{lat}, {lon}",
    }
    return render(request, "wind_trend/wind_trend.html", context)
