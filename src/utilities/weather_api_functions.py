# Libraries ----
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime
from tqdm import tqdm
import os
from pathlib import Path
from typing import List
import glob
from dotenv import load_dotenv

# API Key ----
OPEN_WEATHER_API_KEY = os.getenv("OPEN_WEATHER_API_KEY")

# Function: Kelvin to Fahrenheit ----
def get_kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


# COUNTRY = "US"
# STATE = "MD"
# CITY = "Odenton"
# API_KEY = OPEN_WEATHER_API_KEY
# URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY},{STATE},{COUNTRY}&appid={API_KEY}"

# # - Requests ----
# try:
#     response = requests.get(URL)
# except requests.exceptions.RequestException as e:
#     print("Error: Cannot connect to the weather API.")
#     print(e)


# # - Status Code & Response ----
# if response.status_code != 200:
#     print("Error: Status code != 200.")
#     print(response.text)

# # - Response ----
# response_data = json.loads(response.text)

# request_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# city_name = response_data["name"]
# city_id = response_data["id"]
# city_country = response_data["sys"]["country"]
# longitude = response_data["coord"]["lon"]
# latitude = response_data["coord"]["lat"]
# weather_description = response_data["weather"][0]["description"]
# temp_farenheit = get_kelvin_to_fahrenheit(response_data["main"]["temp"])
# temp_min_farenheit = get_kelvin_to_fahrenheit(response_data["main"]["temp_min"])
# temp_max_farenheit = get_kelvin_to_fahrenheit(response_data["main"]["temp_max"])
# humidity = response_data["main"]["humidity"]
# wind_speed = response_data["wind"]["speed"]

# df = pd.DataFrame({
#     "request_datetime": [request_datetime],
#     "city_name": [city_name],
#     "city_id": [city_id],
#     "city_country": [city_country],
#     "longitude": [longitude],
#     "latitude": [latitude],
#     "weather_description": [weather_description],
#     "temp_farenheit": [temp_farenheit],
#     "temp_min_farenheit": [temp_min_farenheit],
#     "temp_max_farenheit": [temp_max_farenheit],
#     "humidity": [humidity],
#     "wind_speed": [wind_speed]
# })



# Function: Get Weather Data ----
# def get_current_weather_data(country = "US", state = "MD", city = "Odenton"):

#     api_key = "b16a6367cbf0bf2af37e54a098c9bcc6"

#     URL = f"https://api.openweathermap.org/data/2.5/weather?q={city},{state},{country}&appid={api_key}"

#     # - Requests ----
#     try:
#         response = requests.get(URL)
#     except requests.exceptions.RequestException as e:
#         print("Error: Cannot connect to the weather API.")
#         print(e)
#         return None

#     # - Status Code & Response ----
#     if response.status_code != 200:
#         print("Error: Status code != 200.")
#         print(response.text)
#         return None

#     # - Response ----
#     response_data = json.loads(response.text)

#     # - Data ----
#     request_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     city_name = response_data["name"]
#     city_id = response_data["id"]
#     city_country = response_data["sys"]["country"]
#     longitude = response_data["coord"]["lon"]
#     latitude = response_data["coord"]["lat"]
#     weather_description = response_data["weather"][0]["description"]
#     temp_farenheit = get_kelvin_to_fahrenheit(response_data["main"]["temp"])
#     temp_min_farenheit = get_kelvin_to_fahrenheit(response_data["main"]["temp_min"])
#     temp_max_farenheit = get_kelvin_to_fahrenheit(response_data["main"]["temp_max"])
#     humidity = response_data["main"]["humidity"]
#     wind_speed = response_data["wind"]["speed"]

#     df = pd.DataFrame({
#         "request_datetime": [request_datetime],
#         "city_name": [city_name],
#         "city_id": [city_id],
#         "city_country": [city_country],
#         "longitude": [longitude],
#         "latitude": [latitude],
#         "weather_description": [weather_description],
#         "temp_farenheit": [temp_farenheit],
#         "temp_min_farenheit": [temp_min_farenheit],
#         "temp_max_farenheit": [temp_max_farenheit],
#         "humidity": [humidity],
#         "wind_speed": [wind_speed]
#     })

#     return df

def get_current_weather_data(country="US", state="MD", city="Odenton", verbose=False):
    """
    Fetch current weather data for a given location with optional verbose output.

    Args:
        country (str): Country code (default: "US")
        state (str): State code (default: "MD")
        city (str): City name (default: "Odenton")
        verbose (bool): Whether to print detailed progress (default: False)

    Returns:
        pandas.DataFrame: Weather data or None if error occurs
    """

    if verbose:
        print(f"\nFetching weather data for {city}, {state}, {country}...")

    # api_key = "b16a6367cbf0bf2af37e54a098c9bcc6"
    api_key = OPEN_WEATHER_API_KEY
    URL = f"https://api.openweathermap.org/data/2.5/weather?q={city},{state},{country}&appid={api_key}"

    # Initialize progress bar
    progress = tqdm(total=4, disable=not verbose, desc="Processing")

    # - Requests ----
    try:
        if verbose:
            print("\nMaking API request...")
        response = requests.get(URL)
        progress.update(1)
    except requests.exceptions.RequestException as e:
        print("Error: Cannot connect to the weather API.")
        print(e)
        return None

    # - Status Code & Response ----
    if verbose:
        print("Checking response status...")
    if response.status_code != 200:
        print("Error: Status code != 200.")
        print(response.text)
        return None
    progress.update(1)

    # - Response ----
    if verbose:
        print("Parsing JSON response...")
    response_data = json.loads(response.text)
    progress.update(1)

    # - Data ----
    if verbose:
        print("Extracting weather data...")

    request_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    city_name = response_data["name"]
    city_id = response_data["id"]
    city_country = response_data["sys"]["country"]
    longitude = response_data["coord"]["lon"]
    latitude = response_data["coord"]["lat"]
    weather_description = response_data["weather"][0]["description"]
    temp_farenheit = get_kelvin_to_fahrenheit(response_data["main"]["temp"])
    temp_min_farenheit = get_kelvin_to_fahrenheit(response_data["main"]["temp_min"])
    temp_max_farenheit = get_kelvin_to_fahrenheit(response_data["main"]["temp_max"])
    humidity = response_data["main"]["humidity"]
    wind_speed = response_data["wind"]["speed"]

    df = pd.DataFrame({
        "request_datetime": [request_datetime],
        "city_name": [city_name],
        "city_id": [city_id],
        "city_country": [city_country],
        "longitude": [longitude],
        "latitude": [latitude],
        "weather_description": [weather_description],
        "temp_farenheit": [temp_farenheit],
        "temp_min_farenheit": [temp_min_farenheit],
        "temp_max_farenheit": [temp_max_farenheit],
        "humidity": [humidity],
        "wind_speed": [wind_speed]
    })

    progress.update(1)
    progress.close()

    if verbose:
        print(f"\nSuccessfully retrieved weather data for {city_name}!")

    return df

# data = get_current_weather_data(verbose=True)


# Function: Save Weather Data ----
def get_save_weather_data(data: pd.DataFrame):
    """
    Save weather data to a CSV file.

    Args:
        data (pandas.DataFrame): Weather data
    """

    data = data.copy()

    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    file_name = f"data/open_weather_data/open_weather_data_{current_timestamp}.csv"
    data.to_csv(file_name, index=False)

    print(f"\nWeather data saved to {file_name}!")

# get_save_weather_data(data)


# Function: Combine Weather Data ----
def get_combine_weather_data(
    data_path: str = "data/open_weather_data/",
    verbose: bool = False,
    overwrite: bool = True

) -> None:
    """
    Combines individual weather data CSV files into a single combined file.

    Args:
        data_path (str): Path to the directory containing weather data CSV files
        verbose (bool): Whether to print detailed progress messages
    """
    # Convert to Path object for better path handling
    path = Path(data_path)
    output_file = path / "open_weather_data_combined.csv"

    # Check if directory exists
    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {data_path}")

    # Check if output file exists
    # if output_file.exists():
    #     while True:
    #         response = input(f"\nWarning: {output_file} already exists.\nDo you want to override it? (yes/no): ").lower()
    #         if response in ['yes', 'no']:
    #             if response == 'no':
    #                 print("Operation cancelled.")
    #                 return
    #             break
    #         print("Please enter 'yes' or 'no'")

    if output_file.exists() and not overwrite:
        print(f"Output file {output_file} already exists. Skipping...")
        return



    # # Get list of CSV files excluding the combined file
    csv_files = [f for f in path.glob("*.csv") if not f.name.endswith("combined.csv")]

    if not csv_files:
        print(f"No CSV files found in {data_path}")
    else:
        print(f"\nFound {len(csv_files)} CSV files to combine")
        # return

    if verbose:
        print(f"\nFound {len(csv_files)} CSV files to combine")

    # Initialize empty list to store dataframes
    dfs: List[pd.DataFrame] = []

    # Read each CSV file
    for file in csv_files:
        try:
            if verbose:
                print(f"Reading {file.name}...")
            df = pd.read_csv(file)

            # Verify the file has data
            if len(df) > 0:
                dfs.append(df)
            else:
                print(f"Warning: {file.name} is empty")

        except Exception as e:
            print(f"Error reading {file.name}: {str(e)}")
            continue

    if not dfs:
        print("No valid data found in CSV files")
        # return

    # Combine all dataframes
    if verbose:
        print("\nCombining data...")

    combined_df = pd.concat(dfs, ignore_index=True)

    # Sort by datetime if it exists
    if 'request_datetime' in combined_df.columns:
        combined_df = combined_df.sort_values('request_datetime')

    # Save combined data
    try:
        if verbose:
            print(f"Saving combined data to {output_file}")
        combined_df.to_csv(output_file, index=False)
        print(f"\nSuccessfully combined {len(dfs)} files into {output_file}")
        print(f"Total rows: {len(combined_df)}")
    except Exception as e:
        print(f"Error saving combined file: {str(e)}")

# get_combine_weather_data(verbose=True, overwrite=True)

