from decimal import Decimal

import libgeohash
import pytest
from dynamodb_geo import GeoPosition
from dynamodb_geo.configuration import StatisticsConfiguration
from dynamodb_geo.model import GeoBoundingBox
from dynamodb_geo.statistics import StatisticsTable
from shapely.geometry import box

STATISTICS_CONFIG = StatisticsConfiguration(
    precision_steps=(3, 6),
)
GEOHASH = 'u281z7j7ppzs'
OTHER_GEOHASH = 'u281hsd54tnu'
GEOHASH_PREFIX = 'u28'


@pytest.fixture
def statistics_table(setup_dynamodb, local_dynamodb_client, local_dynamodb_resource):
    return StatisticsTable(
        statistics_table_name=pytest.STATISTICS_TABLE_NAME,
        statistics_config=STATISTICS_CONFIG,
        dynamo_client=local_dynamodb_client,
        dynamo_resource=local_dynamodb_resource,
    )


def test_query_returns_nothing_for_empty_table(statistics_table):
    result = statistics_table.query(box(10.0, 48.0, 11.0, 49.0))

    assert len(result.items) == 0


def test_query_for_small_box_returns_high_res_results(statistics_table, insert_item):
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(48.1, 10.1, 3, 5))
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(48.1, 10.1, 6, 5))

    result = statistics_table.query(box(10.099, 48.099, 10.101, 48.101))

    assert len(result.items) == 1
    assert result.items[0].item_count == 5
    assert result.items[0].geohash == 'u0x1tu'
    assert result.items[0].center == GeoPosition(Decimal('48.10089111328125'), Decimal('10.1019287109375'))
    assert result.items[0].boundaries == GeoBoundingBox(
        north=48.1036376953125, south=48.09814453125, west=10.096435546875, east=10.107421875)


def test_query_for_large_box_returns_low_res_results(statistics_table, insert_item):
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(48.1, 10.1, 3, 5))
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(48.1, 10.1, 6, 5))

    result = statistics_table.query(box(10.0, 48.0, 11.0, 49.0))

    assert len(result.items) == 1
    assert result.items[0].item_count == 5
    assert result.items[0].geohash == 'u0x'


def test_query_many_items(statistics_table, insert_item):
    for i in range(15):
        for j in range(15):
            insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(48 + 0.01 * i, 10 + 0.01 * j, 6, 5))

    result = statistics_table.query(box(10.000, 48.000, 10.101, 48.101))

    assert len(result.items) == 110


def get_stat_item(lat: float, lon: float, precision: int, count: int):
    geohash = libgeohash.encode(lat, lon, precision=precision)
    return {'_geohash_prefix': geohash[0:3], '_geohash': geohash, 'item_count': count}
