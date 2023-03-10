import pandas as pd

from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@flow(retries = 3)
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read data from web into Pandas DataFrame'''

    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)

    return df

@task(log_prints = True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    '''Fix dtype issues'''

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    print(df.head(2))
    print(f'Columns {df.dtypes}')
    print(f'Count of rows: {len(df)}')

    return df

@task(log_prints = True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    '''Write DataFrame out locally as a parquet file'''

    # path = Path(f'data/{color}/{dataset_file}.parquet')
    path = Path(f'../../data/{color}/{dataset_file}.parquet')
    print(path)
    df.to_parquet(path, compression='gzip')

    return path

@task()
def write_to_gcs(path: Path) -> None:
    '''Uploading local parquet file to GCS'''

    gcp_cloud_storage_bucket_block = GcsBucket.load('dtc-de-gcs')

    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path = f'{path}',
        to_path = path
    )

    return


@flow()
def etl_web_to_gcs() -> None:
    '''This is the main ETL function'''
    color = 'yellow'
    year = 2021
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_to_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()
