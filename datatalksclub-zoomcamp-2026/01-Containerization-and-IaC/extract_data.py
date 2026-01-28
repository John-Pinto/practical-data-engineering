from pathlib import Path
from typing import Literal

import requests

# Creating a data directory
CWD = Path.cwd()
data_dir = CWD.joinpath("data")
try:
    data_dir.mkdir(
        parents=True,
        exist_ok=True,
    )
except Exception as e:
    print(f"Error on creating data directory: {e}")


# Getting all the url that is been requested
def get_nyc_taxi_url(
    year: int,
    month: int,
    taxi_type: Literal["yellow", "green", "fhv", "fhvhv", "zone"],
) -> str:
    """ """
    if not (isinstance(year, int) and isinstance(month, int)):
        raise TypeError(
            f"Error: Received year={year}, month={month}. Year and month needs to be an integer"
        )

    if month not in range(1, 13):
        raise ValueError("Month needs to be between 1 and 12")
    elif month < 10:
        month = f"0{month}"

    if year not in range(2009, 2026):
        raise ValueError("Year needs to be between 2009 and 2025")

    match taxi_type:
        case "yellow":
            data_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
        case "green":
            data_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"
        case "fhv":
            data_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet"
        case "fhvhv":
            data_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet"
        case "zone":
            data_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        case _:
            raise ValueError(
                f"\nInvalid taxi type {taxi_type}, it should be either yellow, green, fhv or fhvhv"
            )

    return data_url


def extract_taxi_data(url: str) -> None | Path:
    """ """
    file_path = data_dir / url.split("/")[-1].strip()

    try:
        response = requests.get(
            url=url,
            timeout=5,
        )

        response.raise_for_status()

        print(f"Connected to {url} successfully.")
        print(f"Starting file download to {file_path}")

        with open(file=file_path, mode="wb+") as file:
            file.write(response.content)

        print(f"File has been downloaded to {file_path}")

        return file_path

    except requests.exceptions.ConnectionError as e:
        print(
            f"\nError: Received an error while connecting to the URL with reason: {e} "
            f"status code: {response.status_code}\n"
        )

    except requests.exceptions.Timeout as e:
        print(f"\nError: Timed out while connecting to the URL: {e}")

    except requests.exceptions.HTTPError as e:
        print(f"\nError: Received HTTP Error with message: {e}")


if __name__ == "__main__":
    url = get_nyc_taxi_url(
        year=2025,
        month=11,
        taxi_type="yellow",
    )
    filepath = extract_taxi_data(url=url)
    print(filepath)
