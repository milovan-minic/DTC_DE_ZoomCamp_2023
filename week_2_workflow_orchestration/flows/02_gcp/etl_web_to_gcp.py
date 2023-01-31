import pandas as pd

from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@flow()
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read data from web into Pandas DataFrame'''

    df = pd.read_csv(dataset_url)

    return df


@flow()
def etl_web_to_gcs() -> None:
    '''This is the main ETL function'''
    color = 'yellow'
    year = 2021
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)


if __name__ == '__main__':
    etl_web_to_gcs()
