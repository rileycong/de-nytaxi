import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(cols):
    snake_cols = []
    for col in cols:
        col = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col)
        snake_cols.append(re.sub('([a-z0-9])([A-Z])', r'\1_\2', col).lower())
    
    return snake_cols

@transformer
def transform(data, *args, **kwargs):
    data = data[data['passenger_count']> 0]
    data = data[data['trip_distance']>0]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data.columns = camel_to_snake(data.columns)
    
    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['vendor_id'] is not None, 'The output is not snake case'
    assert output['passenger_count'].isin([0]).sum() == 0, 'there are rides with 0 passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'there are trips with 0 km'
