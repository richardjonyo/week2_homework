import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame
import os

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
from datetime import datetime

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


# update the variables below
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/personal-gcp.json/dtc-gc-37b8d03b4e65.json'
project_id = 'dtc-gc'
bucket_name = 'dtc_data_lake_dtc-gc'
object_key = 'ny_taxi_data.parquet'
table_name = 'ny_taxi_data'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> DataFrame:
    #print(f"Stupid date: {df['tpep_pickup_datetime'][0]}")
    df['tpep_pickupdate'] = df['tpep_pickup_datetime'].dt.date

    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['tpep_pickupdate'],
        filesystem=gcs
    )

    return df.drop(['tpep_pickupdate'], axis=1)

