import os

import click
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError
from tqdm.auto import tqdm

from extract_data import extract_taxi_data, get_nyc_taxi_url

load_dotenv()


@click.command()
@click.option(
    "--pg-user",
    default=os.environ.get(key="POSTGRES_USER"),
    show_default=True,
    help="PostgreSQL user",
)
@click.option(
    "--pg-pass",
    default=os.environ.get(key="POSTGRES_PASSWORD"),
    show_default=True,
    help="PostgreSQL password",
)
@click.option(
    "--pg-host",
    default=os.environ.get(key="PG_HOST"),
    show_default=True,
    help="PostgreSQL host",
)
@click.option(
    "--pg-port",
    default=os.environ.get(key="PG_HOST_PORT"),
    type=int,
    show_default=True,
    help="PostgreSQL database port",
)
@click.option(
    "--pg-db",
    default=os.environ.get(key="POSTGRES_DB"),
    show_default=True,
    help="PostgreSQL database name",
)
@click.option(
    "--year",
    default=2025,
    show_default=True,
    type=int,
    help="Year to download nyc taxi data",
)
@click.option(
    "--month",
    default=1,
    show_default=True,
    type=int,
    help="Month to download nyc taxi data",
)
@click.option(
    "--taxi-type",
    default="yellow",
    show_default=True,
    type=click.Choice(["yellow", "green", "fhv", "fhvhv", "zone"]),
    help="Type of taxi to download nyc taxi data",
)
@click.option(
    "--target-table",
    default="yellow_taxi_data",
    show_default=True,
    help="Table name to save the nyc taxi data",
)
@click.option(
    "--chunksize",
    default=100_000,
    show_default=True,
    type=int,
    help="Chunk size for reading data",
)
def loading_data(
    pg_user: str,
    pg_pass: str,
    pg_host: str,
    pg_port: int,
    pg_db: str,
    year: int,
    month: int,
    taxi_type: str,
    target_table: str,
    chunksize: int,
) -> None:
    # Extract the nyc taxi data
    url = get_nyc_taxi_url(year=year, month=month, taxi_type=taxi_type)
    data_filepath = extract_taxi_data(url=url)

    # Creating the postgresql engine to connect and load the data
    database_url = URL.create(
        drivername="postgresql",
        username=pg_user,
        password=pg_pass,
        host=pg_host,
        port=pg_port,
        database=pg_db,
    )

    try:
        engine = create_engine(url=database_url)
        with engine.connect() as _:
            print(f"Connected successfully to the postgresql database: {database_url}")
    except OperationalError as e:
        print(f"Database connection failed: {e}")

    # Reading the data from csv or parquet file
    if data_filepath and data_filepath.exists():
        filename = data_filepath.name
    else:
        raise FileNotFoundError(f"{data_filepath} does not exists.")

    if filename.endswith(".csv"):
        df_iter = pd.read_csv(
            filepath_or_buffer=data_filepath,
            iterator=True,
            chunksize=chunksize,
        )
    elif filename.endswith(".parquet"):
        file = pq.ParquetFile(source=data_filepath)
        df_iter = file.iter_batches(batch_size=chunksize)
    else:
        raise ValueError("File format not supported, only csv and parquet is accepted.")

    # Creating the table and loading the data in postgresql
    first_chunck = True
    print("Starting the data loading process in Postgresql...")

    for df_chunck in tqdm(
        df_iter,
        desc="Processing Chunck",
    ):
        if filename.endswith(".parquet"):
            df_chunck = df_chunck.to_pandas()

        if first_chunck:
            df_chunck.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
            )
            print(f'\nTable: "{target_table}" created in database: "{pg_db}"')

            first_chunck = False

        df_chunck.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
        )

    print("Data is successfully load in Postgresql database")


if __name__ == "__main__":
    loading_data()
