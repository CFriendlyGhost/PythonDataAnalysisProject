import csv
import re
import requests
from geopy.geocoders import Nominatim
import pandas as pd
import statistics
import time
import datetime as dt

REGEX_ONLY_COUNTRY = r"(\b[A-Z][a-z]+\b)"  # e.g. Niemcy;  ; ; ;
REGEX_COUNTRY_ROAD = r"(\b[A-Z][a-z]+\b).+dr\. (\d+)"  # e.g. Niemcy;  ; ; ;  dr. 313;;
REGEX_COUNTRY_HIGHWAY = (
    r"(\b[A-Z][a-z]+\b).+dr\. ([A-Z]\d+)"  # e.g. Niemcy;  ; ; ;  dr. A9;;;
)
REGEX_COUNTRY_CITY = (
    r"(\b[A-Z][a-z]+\b).+m\. (\b[A-Z][a-z]+\b)"  # e.g. Niemcy;  ; ; ;m. Worms;
)


def read_csv():
    data_frame = pd.read_csv(r"Odcinki tras dla pojazdu.csv", sep=";")
    return data_frame


def check_if_poland(location):
    return re.findall(r"Polska", location)


def get_poland_zip(location):
    zip_code_regex = r"\b\d{2}-\d{3}\b"
    zip_code = re.findall(zip_code_regex, location)
    return f"Polska, {zip_code}"


def get_location_outside_poland(location):
    matched_regex = None

    if re.search(
            REGEX_COUNTRY_CITY, location
    ):  # firstly I check if there is a city name in location, as it has the most accurate coordinates
        matched_regex = re.search(
            REGEX_COUNTRY_CITY, location
        )
        return convert_location_to_coordinates(
            f"{matched_regex.group(1)}, {matched_regex.group(2)}"
        )

    elif re.search(
            REGEX_COUNTRY_HIGHWAY, location
    ):
        matched_regex = re.search(
            REGEX_COUNTRY_HIGHWAY, location  # Secondly I try to fit a highway number to
        )  # those I have in my dictionary
        # dictionary is defined in find_highway_coordinates
        if find_highway_coordinates(matched_regex.group(2)) is not None:
            return find_highway_coordinates(matched_regex.group(2))
        else:  # if the highway is not in my dict, I try to find location by geopy module
            return convert_location_to_coordinates(
                f"{matched_regex.group(1)}, {matched_regex.group(2)}"
            )

    elif re.search(REGEX_COUNTRY_ROAD, location):
        matched_regex = re.search(
            REGEX_COUNTRY_ROAD, location
        )  # Thirdly if the road is not highway, it might be a country road, so I checkit
        return (
            convert_location_to_coordinates(
                f"{matched_regex.group(1)}, b{matched_regex.group(2)}"
            )
        )

    elif re.search(REGEX_ONLY_COUNTRY, location):
        matched_regex = re.search(
            REGEX_ONLY_COUNTRY, location
        )  # If there is nothing else excepted country name, I do just getting location of the country
        return convert_location_to_coordinates(
            f"{matched_regex.group(1)}"
        )

    else:
        return matched_regex


def find_highway_coordinates(hi_way_code):
    highway_dict = {
        "a2": [52.430941, 9.714131],
        "a3": [49.786881, 10.191812],
        "a6": [49.316989, 11.036183],
        "a4": [51.069284, 13.633883],
        "a38": [51.447698, 11.584352],
        "a51": [51.154484, 7.263666],
        "a14": [52.510729, 11.734560],
        "a13": [51.736985, 13.867353],
        "a40": [51.427852, 6.543046],
        "a9": [51.424466, 6.511698],
        "a7": [51.409836, 9.825937],
        "a12": [52.337269, 14.151682],
    }

    return highway_dict.get(hi_way_code)


def convert_location_to_coordinates(
        location,
):  # weather API requires coordinates location, so I need to convert location name such as city name to coordinates
    geolocator = Nominatim(
        user_agent="MSiD_Proj"
    )
    coordinates = geolocator.geocode(location)
    if coordinates is not None:
        return coordinates.latitude, coordinates.longitude
    else:
        return None


def get_weather_data(latitude, longitude, start_date, end_date):
    start_date_obj = dt.datetime.strptime(start_date, "%d.%m.%Y %H:%M:%S")
    end_date_obj = dt.datetime.strptime(start_date, "%d.%m.%Y %H:%M:%S")
    required_form_start_date = start_date_obj.strftime(
        "%Y-%m-%d"
    )  # weather api required other date style, then I have in CSV file data
    required_form_end_date = end_date_obj.strftime(
        "%Y-%m-%d"
    )
    difference = end_date_obj - start_date_obj
    hours_diff = int(difference.total_seconds() / 3600)

    url = (
        f"https://archive-api.open-meteo.com/v1/era5?latitude={latitude}&longitude={longitude}"
        f"&start_date={required_form_start_date}"
        f"&end_date={required_form_end_date}&hourly=temperature_2m&hourly=relativehumidity_2m"
        f"&hourly=windspeed_10m&hourly=precipitation"
    )
    response = requests.get(url)
    data = response.json()
    # To have a ready form of weather data I instantly change it to dict style with proper names
    return define_weather_dict(
        data, hours_diff, start_date_obj, start_date, end_date, difference
    )


def define_weather_dict(
        data, hours_diff, start_date_obj, start_date, end_date, difference
):
    if (
            hours_diff > 0
    ):  # It is important to check if the route time was longer than hour,
        return {  # because in other way I just got one data for each dependency, as I got hourly data
            "latitude": data[  # If there is only one hour, there is no sense in calculating mean of readings
                "latitude"
            ],
            "longitude": data["longitude"],
            "start_time": start_date,
            "end_time": end_date,
            "temperature": statistics.mean(
                data["hourly"]["temperature_2m"][
                    start_date_obj.hour: start_date_obj.hour + difference
                ]
            ),
            "humidity": statistics.mean(
                data["hourly"]["relativehumidity_2m"][
                    start_date_obj.hour: start_date_obj.hour + difference
                ]
            ),
            "windspeed": statistics.mean(
                data["hourly"]["windspeed_10m"][
                    start_date_obj.hour: start_date_obj.hour + difference
                ]
            ),
            "precipitation": statistics.mean(
                data["hourly"]["precipitation"][
                    start_date_obj.hour: start_date_obj.hour + difference
                ]
            ),
        }
    else:
        return {
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "start_time": start_date,
            "end_time": end_date,
            "temperature": data["hourly"]["temperature_2m"][start_date_obj.hour],
            "humidity": data["hourly"]["relativehumidity_2m"][start_date_obj.hour],
            "windspeed": data["hourly"]["windspeed_10m"][start_date_obj.hour],
            "precipitation": data["hourly"]["precipitation"][start_date_obj.hour],
        }


def make_weather_data_file():
    data = read_csv()

    with open("weather_start_city.csv", mode="a", newline="") as file:
        writer = csv.writer(file)

        for index, row in data.iterrows():
            if check_if_poland(row[2]):  # At first, I check location
                location = get_poland_zip(row[2])
                coordinates = convert_location_to_coordinates(location)

            else:
                coordinates = get_location_outside_poland(row[2])

            if coordinates is not None:
                time.sleep(0.9)  # other way the api stops working after few GETs
                try:
                    weather_dict = get_weather_data(
                        coordinates[0],
                        coordinates[1],
                        row[1],
                        row[3],  # In weather data dictionary I need also
                    )  # a data such as start_time and end_time
                    writer.writerow(  # so I can to merge the tables later
                        weather_dict.values()  # In other words start_time is as a primary key
                    )
                except (
                        RuntimeError
                ) as e:
                    print("Not Respond", e)


if __name__ == "__main__":
    try:
        make_weather_data_file()
    except RuntimeError as e1:
        print("Not Respond")
