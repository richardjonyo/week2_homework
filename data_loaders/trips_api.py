import io
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    parquet_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
    response = requests.get(parquet_url)
    parquet_bytes = BytesIO(response.content)
    table = pq.read_table(parquet_bytes)

    df = table.to_pandas()

    return df;

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
