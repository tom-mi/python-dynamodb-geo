from decimal import Decimal

import pytest
from dynamodb_geo.configuration import GeoTableConfiguration
from dynamodb_geo.table import GeoTable

CONFIG = GeoTableConfiguration(partition_key_field='id')
LAT = Decimal('48.137154')
LON = Decimal('11.576124')
GEOHASH = 'u281z7j7ppzs'
GEOHASH_PREFIX = 'u28'
ITEM = {
    'id': '1',
    'position': {'latitude': LAT, 'longitude': LON},
    'foo': 'bar',
}


def test_save(setup_dynamodb, local_dynamodb_client, local_dynamodb_resource, get_all_items):
    table = GeoTable(table_name=pytest.TABLE_NAME, config=CONFIG, dynamo_client=local_dynamodb_client,
                     dynamo_resource=local_dynamodb_resource)

    table.put_item(item=ITEM)

    items = get_all_items(pytest.TABLE_NAME)
    assert len(items) == 1

    assert items[0]['id'] == ITEM['id']
    assert items[0]['position'] == ITEM['position']
    assert items[0]['foo'] == ITEM['foo']
    assert items[0]['_geohash_prefix'] == GEOHASH_PREFIX
    assert items[0]['_geohash'] == GEOHASH
