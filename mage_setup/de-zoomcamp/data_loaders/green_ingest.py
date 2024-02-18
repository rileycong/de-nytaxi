import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):

    taxi_dtypes = {
                'VendorID': pd.Int64Dtype(),
                'store_and_fwd_flag': str,
                'RatecodeID': pd.Int64Dtype(),
                'PULocationID':pd.Int64Dtype(),
                'DOLocationID':pd.Int64Dtype(),
                'passenger_count': pd.Int64Dtype(),
                'trip_distance': float,
                'fare_amount': float,
                'extra':float,
                'mta_tax':float,
                'tip_amount':float,
                'tolls_amount':float,
                'ehail_fee': float,
                'improvement_surcharge':float,
                'total_amount':float,
                'payment_type': pd.Int64Dtype(),
                'trip_type': pd.Int64Dtype(),
                'congestion_surcharge':float
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    datetime = kwargs.get('execution_date').strftime("%Y-%m")
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{datetime}.csv.gz'
    df = pd.read_csv(url, sep=",", compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'