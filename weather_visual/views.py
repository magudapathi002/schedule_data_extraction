from django.shortcuts import render
import json

from forecast_process.models import WindData10m


def wind_trend_view(request):
    # Example: Fixed location (adjust if needed)
    lat, lon = 8.25, 76.25

    data = (
        WindData10m.objects
        .filter(latitude=lat, longitude=lon)
        .order_by("valid_time")
        .values("valid_time", "wind_speed")
    )

    # Format data for Chart.js
    times = [entry["valid_time"].strftime("%Y-%m-%d %H:%M") for entry in data]
    speeds = [entry["wind_speed"] for entry in data]

    context = {
        "labels": json.dumps(times),
        "values": json.dumps(speeds),
        "location": f"{lat}, {lon}"
    }
    return render(request, "wind_trend.html", context)
