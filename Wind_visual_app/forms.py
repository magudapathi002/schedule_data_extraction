from django import forms

class WindSpeedQueryForm(forms.Form):
    LEVEL_CHOICES = [
        (10, "10"),
        (80, "80"),
        (100, "100"),
        (600, "600"),
        (850, "850"),
    ]

    latitudes = forms.CharField(
        label="Latitudes",
        help_text="Enter latitudes (e.g. 11.5)",
    )
    longitudes = forms.CharField(
        label="Longitudes",
        help_text="Enter longitudes (e.g. 77.2)",
    )
    level = forms.ChoiceField(choices=LEVEL_CHOICES, label="Wind Level (m)")

    from_datetime = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'}),
        label="From"
    )
    to_datetime = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'}),
        label="To"
    )
