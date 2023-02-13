from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

@task(retries = 3, cache_key_fn = task_input_hash, cache_expiration = timedelta(days = 1))
def fetch(dataset_url: str) -> pd.DataFrame:
    ''' Read taxi data from web into pandas DataFrame '''

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints = True)
def clean(df : pd.DataFrame) -> pd.DataFrame:
    ''' Fix dtype issues '''

    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    df['PUlocationID'] = df['PUlocationID'].fillna(-1)
    df['DOlocationID'] = df['DOlocationID'].fillna(-1)
    df['PUlocationID'] = df['PUlocationID'].astype('int')
    df['DOlocationID'] = df['DOlocationID'].astype('int')
    print(df.head(5))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df

@task()
def write_local(df : pd.DataFrame, color : str, dataset_file : str) -> Path:
    ''' Write DataFrame out locally as parquet file '''
    
    path = Path(f'C:/Users/vsbazyrov/de_zoomcamp/de_zoomcamp_homeworks/hw_3/data/{color}/{dataset_file}.parquet')
    df.to_parquet(path, compression = 'gzip')
    return path

@task(retries = 3)
def write_gcs(path : Path, color : str, dataset_file: str) -> None:
    ''' Upload local parquet file to GCS '''

    gcs_block = GcsBucket.load('zoom-gcs')
    gcs_block.upload_from_path(
        from_path = path,
        to_path = Path(f'data/{color}/{dataset_file}.parquet').as_posix()
    )
    return

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    ''' The main ETL function '''

    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, color, dataset_file)

@flow()
def etl_parent_flow(
    months: list[int] = [i for i in range(1, 13)], year: int = 2019, color: str = 'fhv'
) -> None:
    ''' The parent ETL function '''
    
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == '__main__':
    color = 'fhv'
    months = [i for i in range(1, 13)]
    year = 2019
    etl_parent_flow()