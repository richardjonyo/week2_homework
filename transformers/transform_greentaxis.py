if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    print(f'Records before removing zero passengers and trip_distances: {data.shape[0]}')

    df_zero = data[data['passenger_count'].isin([0]) | data['trip_distance'].isin([0])]
   
    # Get the index of records to be removed
    indices_to_remove = df_zero.index

    # Remove the records from the main DataFrame
    df_clean = data.drop(indices_to_remove)
    print(f'Records removed with zero passengers or trip distance: {df_zero.shape[0]}')

    
    # Create a new column lpep_pickup_date 
    df_clean['lpep_pickup_date'] = df_clean['lpep_pickup_datetime'].dt.date

    # Rename columns from Camel Case to Snake Case
    nocols_renamed = df_clean.rename(columns=lambda x: x[0].lower() + x[1:] if x.isalnum() else x.lower(), inplace=True)
    print(f"Columns renamed: {nocols_renamed}")

    #print(data [['passenger_count', 'trip_distance']] <=0)
    print(f'Records after removing zero passengers and trip_distances: {df_clean.shape[0]}')


    return df_clean


@test
def test_output(output, *args) -> None:
    
    assert output['vendorID'] is not None, 'The output is undefined'

    assert output ['passenger_count'].isin([0]).sum()==0, 'There are rides with 0 passengers!'

    assert output ['trip_distance'].isin([0]).sum()==0, 'There are trips with 0 distance!'