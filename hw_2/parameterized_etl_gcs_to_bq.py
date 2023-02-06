from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta
import ssl

@task(retries = 3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    ''' Download trip data from GCS '''

    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('zoom-gcs')
    gcs_block.get_directory(
        from_path = gcs_path,
        local_path = f'C:/Users/vsbazyrov/de_zoomcamp/from_gcs/'
    )
    return Path(f'C:/Users/vsbazyrov/de_zoomcamp/from_gcs/{gcs_path}')

@task(log_prints = True, cache_key_fn = task_input_hash, cache_expiration = timedelta(days = 1))
def not_transform(path: Path) -> pd.DataFrame:
    ''' Read taxi data from GCS into pandas DataFrame '''
    
    df = pd.read_parquet(path)
    print(f'rows: {len(df)}')
    return df

@task
def write_bq(df : pd.DataFrame) -> None:
    ''' Write DataFrame to BigQuery '''

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")

    df.to_gbq(
        destination_table = 'trips_data_all.rides',
        project_id = 'ny-rides-lantenak',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists = 'append'
    )

@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:
    ''' Main ETL flow to load data into Big Query '''

    path = extract_from_gcs(color, year, month)
    df = not_transform(path)
    write_bq(df)

@flow()
def etl_parent_flow_2(
    months: list[int] = [2, 3], year: int = 2019, color: str = 'yellow'
) -> None:
    ''' Parent ETL flow to load data into BigQuery '''
    
    for month in months:
        etl_gcs_to_bq(year, month, color)

if __name__ == '__main__':
    color = 'yellow'
    months = [2, 3]
    year = 2019
    etl_parent_flow_2()