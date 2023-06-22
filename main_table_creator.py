import csv
import statistics
import data_collecting_methods as IOm
import pandas as pd


def define_dict(start_date, end_date, temperature, humidity, wind_speed, precipitation):
    all_data_dict = {
        "start_time": start_date,
        "end_time": end_date,
        "temperature": temperature,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "precipitation": precipitation,
        "route_length": 0.0,  # default values, will be updated later, when the tables will be merged
        "average_speed": 0.0,
        "ave_fuel_cons_per_100km": 0.0,
        "speedometer": 0,
    }

    return all_data_dict


def find_index_with_date(weather_list, date):   # When I get a date from one table I need to find a record
    for i in range(len(weather_list)):          # with the same date in the second one
        if weather_list[i]["start_time"] == date:
            return i

    return None


def join_weather_and_route_tables():
    combined_data = []

    with open("weather_start_city.csv", "r") as weather_file:   # At first, I read weather data from starting city
        read_data = csv.reader(weather_file, delimiter=",")
        next(read_data)
        for row in read_data:
            combined_data.append(
                define_dict(
                    row[2],
                    row[3],
                    float(row[4]),
                    float(row[5]),
                    float(row[6]),
                    float(row[7]),
                )
            )

    with open("weather_destination_city.csv", "r") as weather_file:     # Then I read data  weather from destination
        read_data = csv.reader(weather_file, delimiter=",")     # city and combined two of those
        for row in read_data:
            index = find_index_with_date(combined_data, row[2])
            if index is not None:
                combined_data[index]["temperature"] = statistics.mean(
                    [combined_data[index]["temperature"], float(row[4])]
                )
                combined_data[index]["humidity"] = statistics.mean(
                    [combined_data[index]["humidity"], float(row[5])]
                )
                combined_data[index]["wind_speed"] = statistics.mean(
                    [combined_data[index]["wind_speed"], float(row[6])]
                )
                combined_data[index]["precipitation"] = statistics.mean(
                    [combined_data[index]["precipitation"], float(row[7])]
                )

    routs_data = IOm.read_csv()
    for index, row in routs_data.iterrows():    # At the end the route data is added
        index = find_index_with_date(combined_data, row[1])

        if index is not None:
            combined_data[index]["route_length"] = float(row[5].replace(",", "."))
            combined_data[index]["average_speed"] = float(row[8].replace(",", "."))
            combined_data[index]["ave_fuel_cons_per_100km"] = float(
                row[11].replace(",", ".")
            )
            combined_data[index]["speedometer"] = int(row[12])

    with open("weather_route_data.csv", "w", newline="") as weather_file:
        writer = csv.DictWriter(weather_file, fieldnames=combined_data[0].keys())
        writer.writeheader()     # As everything is combined in combined_data list I save it to new CSV file
        writer.writerows(combined_data)


def specify_season_based_on_day_month(month, day):
    # astronomical dates of the seasons
    if (
        (month == 3 and day >= 20)
        or (month == 4)
        or (month == 5)
        or (month == 6 and day < 21)
    ):
        return 1
    elif (
        (month == 6 and day >= 21)
        or (month == 7)
        or (month == 8)
        or (month == 9 and day < 23)
    ):
        return 2
    elif (
        (month == 9 and day >= 23)
        or (month == 10)
        or (month == 11)
        or (month == 12 and day < 21)
    ):
        return 3
    else:
        return 4


def create_season_column():
    # This function get a ready table, and it adds a season column
    # it shows seasons in numerical way as a:
    # 1 - spring
    # 2 - summer
    # 3 - autumn
    # 4 - winter
    # numerical way will be helpful when data will be analysed

    df = pd.read_csv("weather_rout_data.csv", header=0)
    df["start_time"] = pd.to_datetime(df["start_time"], format="%d.%m.%Y %H:%M:%S")
    df["season"] = df["start_time"].apply(
        lambda date: specify_season_based_on_day_month(date.month, date.day)
    )
    df.to_csv("weather_route_data.csv", index=False)


if __name__ == "__main__":
    join_weather_and_route_tables()
    create_season_column()
