import io
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green'
    # Files are names as: green_tripdata_2020-08.csv.gz 
  
    months = {10, 11, 12}
    year = "2020"

    #define schema for the taxi data
    taxi_dtypes = {
        'VendorID': 'Int64',
        'store_and_fwd_flag': 'str',
        'RatecodeID': 'Int64',
        'PULocationID': 'Int64',
        'DOLocationID': 'Int64',
        'passenger_count': 'Int64',
        'trip_distance': 'float64',
        'fare_amount': 'float64',
        'extra': 'float64',
        'mta_tax': 'float64',
        'tip_amount': 'float64',
        'tolls_amount': 'float64',
        'ehail_fee': 'float64',
        'improvement_surcharge': 'float64',
        'total_amount': 'float64',
        'payment_type': 'float64',
        'trip_type': 'float64',
        'congestion_surcharge': 'float64'
    }

    # Parse the dates
    parse_dates_green_taxi = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    # Initialize an empty list to store DataFrames
    dfs = []

    # Loop throught the months
    for month in months: 
        taxi_file_url = f"{url}/green_tripdata_{year}-{month}.csv.gz" 
        print (f"File url: {taxi_file_url}")
        df = pd.read_csv(taxi_file_url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates_green_taxi)
        dfs.append(df)

    # Concatenate the DataFrames
    merged_df = pd.concat(dfs, ignore_index=True)

    return merged_df



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
