import pandas as pd

from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries = 3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    '''Download data from GCS'''
    gcs_path = f'../../data/{color}/{color}_tripdata_{year}-{month:02}.parquet' # GCS path

    gcs_block = GcsBucket.load('dtc-de-gcs')
    gcs_block.get_directory(from_path = gcs_path, local_path = f'../data/') # local filesystem path
    
    return Path(f'../data/{gcs_path}')

@task(retries = 3)
def transform(path: Path) -> pd.DataFrame:
    '''Data cleaning example'''
    df = pd.read_parquet(path)

    print(f"Pre: missing passengers count => {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace = True)
    print(f"Post: missing passengers count => {df['passenger_count'].isna().sum()}")

    return df

@task(retries = 3)
def write_to_bq(df: pd.DataFrame) -> None:
    '''Write DataFrame to BigQuery'''

    gcp_credentials_block = GcpCredentials.load("dtc-de-creds")

    df.to_gbq(
        destination_table = 'dtc_de_bq_dataset_2.yellow_taxi_rides',
        project_id = 'ace-element-375718',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 100_000,
        if_exists = 'append'
    )

@flow()
def etl_gcs_to_bq():
    '''Main flow to load data into BigQuery Data Warehouse'''
    color = 'yellow'
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_to_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()