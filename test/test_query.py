import logging
from decimal import Decimal

import libgeohash
import pytest
import shapely.geometry
from dynamodb_geo.configuration import GeoTableConfiguration
from dynamodb_geo.table import GeoTable
from shapely.geometry import box


@pytest.mark.parametrize('polygon, geohash, expected_error', [
    (box(10.0, 48.0, 11.0, 49.0), '12345678901', 'Cannot query by both geohash and polygon.'),
    (None, None, 'Exactly one of geohash or polygon must be specified as query parameter.'),
])
def test_query_fails_for_invalid_parameter(polygon, geohash, expected_error, local_dynamodb_client,
                                           local_dynamodb_resource):
    config = GeoTableConfiguration(partition_key_field='id', precision=10)
    table = GeoTable(table_name=pytest.TABLE_NAME, config=config, dynamo_client=local_dynamodb_client,
                     dynamo_resource=local_dynamodb_resource)

    with pytest.raises(ValueError) as e:
        table.query(polygon=polygon, geohash=geohash, limit=4)

    assert str(e.value) == expected_error


@pytest.mark.parametrize('polygon, geohash, limit, prefix_length', [
    (box(10.0, 48.0, 11.0, 49.0), None, 20, 2),
    (box(10.0, 48.0, 11.0, 49.0), None, 100, 4),
    (box(10.04, 48.04, 10.07, 48.07), None, 20, 2),
    (box(10.04, 48.04, 10.07, 48.07), None, 20, 4),
    (box(10.001, 48.001, 10.003, 48.003), None, 7, 4),
    (box(10.000, 48.000, 11.11, 48.10), None, 5, 5),
    (shapely.geometry.LinearRing([(10.0, 48.0), (10.1, 48.0), (10.0, 48.1)]), None, 5, 5),
    (None, 'u0x1k3qvkcnd7', 5, 5),
    (None, 'u0x1k3qvkcnd', 5, 5),
    (None, 'u0x1k3q', 5, 5),
    (None, 'u0x1k', 5, 5),
    (None, 'u0x', 5, 4),
])
def test_query(setup_dynamodb, local_dynamodb_resource, local_dynamodb_client, insert_item, polygon, geohash, limit,
               prefix_length):
    config = GeoTableConfiguration(partition_key_field='id', prefix_length=prefix_length)
    items = _setup_test_data(config, insert_item)

    if polygon is not None:
        polygon_for_expected = polygon
    else:
        bounds = libgeohash.bbox(geohash)
        polygon_for_expected = box(bounds['w'], bounds['s'], bounds['e'], bounds['n'])

    expected_ids = [item['id'] for item in items if _is_item_in_polygon(polygon_for_expected, item)]
    table = GeoTable(table_name=pytest.TABLE_NAME, config=config, dynamo_client=local_dynamodb_client,
                     dynamo_resource=local_dynamodb_resource)
    expected_number_of_results = min(limit, len(expected_ids))
    prefixes = set([item['_geohash_prefix'] for item in items if _is_item_in_polygon(polygon_for_expected, item)])
    hashes = set([item['_geohash'] for item in items if _is_item_in_polygon(polygon_for_expected, item)])
    logging.debug(f'test - prefixes {prefixes}')
    logging.debug(f'test - expected_items {len(expected_ids)}')

    result = table.query(polygon=polygon, geohash=geohash, limit=limit)

    assert len(result.items) == expected_number_of_results
    assert set([item['id'] for item in result.items]).issubset(expected_ids)

    # query next page if expected
    if len(expected_ids) > limit:
        assert result.last_evaluated_key is not None
        assert result.last_evaluated_key['id'] == result.items[-1]['id']

        next_result = table.query(polygon=polygon, geohash=geohash, limit=limit,
                                  exclusive_start_key=result.last_evaluated_key)
        assert set([item['id'] for item in next_result.items]).issubset(expected_ids)
        assert set([item['id'] for item in next_result.items]).isdisjoint(set([item['id'] for item in result.items]))

    # query all results by pagination
    all_items = result.items
    while result.last_evaluated_key is not None:
        result = table.query(polygon=polygon, geohash=geohash, limit=limit,
                             exclusive_start_key=result.last_evaluated_key)
        all_items += result.items

    assert len(all_items) == len(expected_ids)
    assert set([item['id'] for item in all_items]) == set(expected_ids)


def _is_item_in_polygon(bounding_box, item):
    lat = item['position']['latitude']
    lon = item['position']['longitude']
    return bounding_box.contains(shapely.geometry.Point(lon, lat))


def _setup_test_data(config: GeoTableConfiguration, insert_item, num_lat=10, num_lon=10):
    items = []
    for i in range(num_lat):
        for j in range(num_lon):
            lat = Decimal(f'48.{i:02d}')
            lon = Decimal(f'10.{j:02d}')
            geohash = libgeohash.encode(lat, lon, precision=config.precision)
            geohash_prefix = geohash[0:config.prefix_length]
            item = {
                'id': f'{i}-{j}',
                'position': {'latitude': lat, 'longitude': lon},
                '_geohash': geohash,
                '_geohash_prefix': geohash_prefix,
            }
            insert_item(pytest.TABLE_NAME, item)
            items.append(item)
    return items
