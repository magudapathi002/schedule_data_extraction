from datetime import date

from wind_data_processing.models import GRIBCycleStatus


def url_generator(url):
    """
    A generator function that yields URLs from a given list of URLs.

    Args:
        url (list): A list of URLs.

    Yields:
        str: A URL from the input list.

    Raises:
        StopIteration: When all URLs have been yielded.
    """


def get_or_create_today_status():
    today = date.today()
    # today = '2025-05-30'
    status_obj, _ = GRIBCycleStatus.objects.get_or_create(date=today)
    return status_obj
