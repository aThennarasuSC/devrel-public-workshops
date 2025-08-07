import os

from airflow.sdk import Asset, dag
from airflow.decorators import task
from pendulum import datetime, duration

_WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude={lat}&longitude={long}&current="
    "temperature_2m,relative_humidity_2m,"
    "apparent_temperature"
)

OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv("OBJECT_STORAGE_PATH_NEWSLETTER", default="include/newsletter")
OBJECT_STORAGE_PATH_USER_INFO = os.getenv("OBJECT_STORAGE_PATH_USER_INFO", default="include/user_data")
OBJECT_STORAGE_LOCATIONS_FILE = os.getenv("OBJECT_STORAGE_LOCATIONS_FILE", default="include/locations.json")


def _get_lat_long(location):
    """
    Note that this version of the function caches the geocoding
    """
    import time
    from airflow.sdk import ObjectStoragePath
    from geopy.geocoders import Nominatim
    import json

    locations_file = ObjectStoragePath(
        f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_LOCATIONS_FILE}",
        conn_id=OBJECT_STORAGE_CONN_ID,
    )
    if not locations_file.exists():
        locations_file.write_text("{}")

    locations_data = json.loads(locations_file.read_text())

    if location in locations_data.keys():
        return tuple(locations_data[location])

    time.sleep(10)  # to respect geopy usage policy
    geolocator = Nominatim(user_agent="MyApp/1.0 (my_email@example.com)")

    location_object = geolocator.geocode(location)

    coordinates = (float(location_object.latitude), float(location_object.longitude))

    locations_data[location] = coordinates

    locations_file.write_text(json.dumps(locations_data))

    return coordinates


@dag(
    start_date=datetime(2025, 3, 1),
    schedule="@daily",
    default_args={
        "retries": 0,
        "retry_delay": duration(minutes=3),
    },
)
def personalize_newsletter():

    @task
    def get_user_info() -> list[dict]:
        """
        Reads user info JSON files from object storage and returns list of user dicts.
        """
        import json
        from airflow.sdk import ObjectStoragePath

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_USER_INFO}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )

        user_info = []
        for file in object_storage_path.iterdir():
            if file.is_file() and file.suffix == ".json":
                bytes_ = file.read_block(offset=0, length=None)
                user_info.append(json.loads(bytes_))

        return user_info

    _get_user_info = get_user_info()

    @task(max_active_tis_per_dag=1, retries=4)
    def get_weather_info(user: dict) -> dict:
        """
        Fetch weather info for the user's location.
        """
        import requests

        lat, long = _get_lat_long(user["location"])
        r = requests.get(_WEATHER_URL.format(lat=lat, long=long))
        user["weather"] = r.json()

        return user

    _get_weather_info = get_weather_info.expand(user=_get_user_info)

    @task(outlets=[Asset("personalized_newsletters")])
    def create_personalized_newsletter(
        user: dict,
        **context: dict,
    ) -> None:
        """
        Creates personalized newsletters by inserting current weather info into
        a generic newsletter template.
        """
        from airflow.sdk import ObjectStoragePath

        # Debug logging (optional)
        context["ti"].log.info(f"triggering_asset_events: {context['triggering_asset_events']}")

        # Safely get run_date from triggering asset events
        triggering_events = context["triggering_asset_events"].get(Asset("selected_quotes"), [])
        if not triggering_events:
            raise ValueError("No triggering asset events found for selected_quotes")

        run_date = triggering_events[0].extra.get("run_date")
        if not run_date:
            raise ValueError("Run date not found in asset event metadata")

        id_ = user["id"]
        name = user["name"]
        location = user["location"]
        actual_temp = user["weather"]["current"]["temperature_2m"]
        apparent_temp = user["weather"]["current"]["apparent_temperature"]
        rel_humidity = user["weather"]["current"]["relative_humidity_2m"]

        new_greeting = (
            f"Hi {name}! \n\nIf you venture outside right now "
            f"in {location}, you'll find the temperature to be "
            f"{actual_temp}°C, but it will feel more like "
            f"{apparent_temp}°C. The relative humidity is "
            f"{rel_humidity}%."
        )

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )

        daily_newsletter_path = object_storage_path / f"{run_date}_newsletter.txt"
        generic_content = daily_newsletter_path.read_text()

        personalized_content = generic_content.replace(
            "Hello Cosmic Traveler,", new_greeting
        )

        personalized_newsletter_path = object_storage_path / f"{run_date}_newsletter_userid_{id_}.txt"
        personalized_newsletter_path.write_text(personalized_content)

    create_personalized_newsletter.expand(user=_get_weather_info)


personalize_newsletter()
