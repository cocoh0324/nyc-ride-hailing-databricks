"""
Downloads the raw data into the `data/raw` directory.

Run this command from the repository root:
    python scripts/download_sources.py
"""

import os
import io
import zipfile
import requests
import pandas as pd
from io import BytesIO
from urllib.request import urlretrieve

def create_folders(base_dir, data_folders):
    for folder in data_folders:
        path = os.path.join(base_dir, folder)
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"Created folder: {path}")

def download_hvfhv_data(year, months, url_template, output_dir):
    for month in months:
        month = str(month).zfill(2)
        url = f'{url_template}{year}-{month}.parquet'
        output_path = os.path.join(output_dir, f"{year}-{month}.parquet")

        if not os.path.exists(output_path):
            print(f"Starting download for {year}-{month}")
            urlretrieve(url, output_path)
            print(f"Completed downloading for {year}-{month}")
        else:
            print(f"File already exists for {year}-{month}, skipping download")

def download_taxi_zone_lookup(url, output_path):
    print(f"Starting download for taxi zone lookup data")
    response = requests.get(url)

    if response.status_code == 200:
        df = pd.read_csv(BytesIO(response.content))
        df.to_parquet(output_path)
        print(f"Completed downloading for taxi zone lookup data in parquet file")
    else:
        print(f"Failed to download the file, status code: {response.status_code}")

def download_taxi_zone_shapefile(url, output_dir):
    print("Starting download for taxi zone shapefile")
    response = requests.get(url)

    if response.status_code == 200:
        os.makedirs(output_dir, exist_ok=True)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for filename in z.namelist():
                with z.open(filename) as f:
                    file_path = os.path.join(output_dir, filename)
                    with open(file_path, 'wb') as out_file:
                        out_file.write(f.read())
        print("Completed downloading and extracting for taxi zone shapefile")
    else:
        print(f"Failed to download the file, status code: {response.status_code}")

def download_pluto(url, output_dir):
    csv_path = os.path.join(output_dir, "pluto.csv")
    print(f"Starting download for Primary Land Use Tax Lot Output (PLUTO) in CSV file")
    urlretrieve(url, csv_path)
    print(f"Completed downloading PLUTO data in CSV file")

    # Convert CSV to Parquet
    parquet_path = os.path.join(output_dir, "pluto.parquet")
    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path)
    print(f"Converted CSV file to parquet file and saved successfully")

def download_weather_data(url, output_path):
    print(f"Starting download for weather data in CSV file")
    response = requests.get(url)

    if response.status_code == 200:
        try:
            df = pd.read_csv(BytesIO(response.content))
            df['DailySnowDepth'] = pd.to_numeric(df['DailySnowDepth'], errors='coerce')
            df['DailySnowDepth'].fillna(0, inplace=True)
            df.to_parquet(output_path, engine='pyarrow')
            print(f"Completed downloading weather data in parquet file")
        except Exception as e:
            print(f"An error occurred while processing the file: {e}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

if __name__ == "__main__":
    base_dir = "../data"
    data_folders = [
        "raw/taxi_zone", "raw/hvfhv_data", "raw/pluto", "raw/weather_data",
        "curated/hvfhv_data", "curated/pluto", "curated/weather_data", "developed/merged_data"
    ]
    
    create_folders(base_dir, data_folders)
    
    YEAR = "2023"
    MONTHS = range(7, 13)
    URL_TEMPLATE_hvfhv = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_"
    hvfhv_output_dir = os.path.join(base_dir, 'raw/hvfhv_data')
    download_hvfhv_data(YEAR, MONTHS, URL_TEMPLATE_hvfhv, hvfhv_output_dir)
    
    URL_taxi_zone_lookup = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    taxi_zone_lookup_output_dir = os.path.join(base_dir, "raw/taxi_zone")
    taxi_zone_lookup_path = os.path.join(taxi_zone_lookup_output_dir, 'taxi_zone_lookup.parquet')
    download_taxi_zone_lookup(URL_taxi_zone_lookup, taxi_zone_lookup_path)
    
    URL_taxi_zone_shapefile = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
    download_taxi_zone_shapefile(URL_taxi_zone_shapefile, taxi_zone_lookup_output_dir)
    
    URL_pluto = "https://data.cityofnewyork.us/api/views/64uk-42ks/rows.csv?accessType=DOWNLOAD"
    download_pluto(URL_pluto, os.path.join(base_dir, "raw/pluto"))
    
    URL_weather = "https://www.ncei.noaa.gov/oa/local-climatological-data/v2/access/2023/LCD_USW00094728_2023.csv"
    weather_output_dir = os.path.join(base_dir, "raw/weather_data")
    weather_csv_path = os.path.join(weather_output_dir, "weather2023.parquet")
    download_weather_data(URL_weather, weather_csv_path)
