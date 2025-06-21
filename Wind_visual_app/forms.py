from django import forms

class WindSpeedQueryForm(forms.Form):
    latitudes = forms.CharField(
        label="Latitudes",
        help_text="Enter comma-separated latitudes (e.g. 11.5,11.6)",
    )
    longitudes = forms.CharField(
        label="Longitudes",
        help_text="Enter comma-separated longitudes (e.g. 77.2,77.3)",
    )
    level = forms.FloatField()
    from_datetime = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'})
    )
    to_datetime = forms.DateTimeField(
        widget=forms.DateTimeInput(attrs={'type': 'datetime-local'})
    )
