# python-dynamodb-geo

Query geo data from an AWS DynamoDB table efficiently using a GSI indexed with geohashes.

Based on [shapely](https://github.com/Toblerity/Shapely) and [libgeohash](https://github.com/bashhike/libgeohash), 
querying DynamoDB with [boto3](https://github.com/boto/boto3).

Not compatible (except maybe by accident) with any of the other DynamoDB geohashing libraries for other languages.

## Installation

As of now, there is no PyPI package. Install via
```
pip install 'git+https://github.com/tom-mi/python-dynamodb-geo.git#egg=dynamodb-geo&subdirectory=src'
```

## Quickstart

This section assumes you have a table named `my-table` with partition (HASH) key `id`, and a geo position stored
in the field `position` in the format `{'latitude': Decimal('48.137154'), 'longitude': Decimal('11.576124')}`.

Those settings and others can be customized via parameters for `GeoTableConfiguration`.

Add a GSI named `geohash` with the partition (HASH) key `_geohash_prefix` and the sort (RANGE) key `_geohash`
to your DynamoDB Table:
```
dynamodb.create_table(
    TableName='my-table',
    AttributeDefinitions=[
        {'AttributeName': 'id', 'AttributeType': 'S'},
        {'AttributeName': '_geohash_prefix', 'AttributeType': 'S'},
        {'AttributeName': '_geohash', 'AttributeType': 'S'},
    ],
    KeySchema=[
        {'AttributeName': 'id', 'KeyType': 'HASH'},
    ],
    GlobalSecondaryIndexes=[{
        'IndexName': 'geohash',
        'KeySchema': [
            {'AttributeName': '_geohash_prefix', 'KeyType': 'HASH'},
            {'AttributeName': '_geohash', 'KeyType': 'RANGE'},
        ],
        'Projection': {
            'ProjectionType': 'ALL',
        },
    }],
)
```

Configure your GeoTable:
```python
from dynamodb_geo import GeoTableConfiguration, GeoTable

config = GeoTableConfiguration(
    partition_key_field='id',
)
geo_table = GeoTable(table_name='my-table', config=config)
```

Save an item:
```python
geo_table.put_item(item={
    'id': 1,
    'position': {'latitude': Decimal('48.137154'), 'longitude': Decimal('11.576124')},
})
```

Query:

```python
from shapely.geometry import box

result = geo_table.query(polygon=box(10.0, 48.0, 11.0, 49.0), limit=20)

print(result.items)
```

Query the next page:
```python
next_result = geo_table.query(polygon=box(10.0, 48.0, 11.0, 49.0), limit=20, exclusive_start_key=result.last_evaluated_key)
```


## Development setup

Install dependencies via Pipenv

```
pipenv install --dev
```

Start local dynamodb
```
docker run -p8000:8000 amazon/dynamodb-local
```

Run tests

```
pipenv run pytest test/
```
