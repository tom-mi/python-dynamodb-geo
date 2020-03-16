from decimal import Decimal

import libgeohash
import pytest
import shapely.geometry
from dynamodb_geo.configuration import GeoTableConfiguration
from dynamodb_geo.table import GeoTable
from shapely.geometry import box


@pytest.mark.parametrize('polygon, limit, prefix_length', [
    (box(10.0, 48.0, 11.0, 49.0), 20, 4),
    (box(10.0, 48.0, 11.0, 49.0), 100, 4),
    (box(10.001, 48.001, 10.003, 48.003), 7, 4),
    (box(10.000, 48.000, 11.11, 48.10), 5, 5),
    (shapely.geometry.LinearRing([(10.0, 48.0), (10.1, 48.0), (10.0, 48.1)]), 5, 5),
])
def test_query(setup_dynamodb, polygon, limit, prefix_length):
    config = GeoTableConfiguration(partition_key_field='id', prefix_length=prefix_length)
    items = _setup_test_data(config)
    expected_ids = [item['id'] for item in items if _is_item_in_polygon(polygon, item)]
    table = GeoTable(table_name=pytest.TABLE_NAME, config=config)
    expected_number_of_results = min(limit, len(expected_ids))
    prefixes = set([item['_geohash_prefix'] for item in items if _is_item_in_polygon(polygon, item)])
    print(prefixes)

    result = table.query(polygon=polygon, limit=limit)

    assert len(result.items) == expected_number_of_results
    assert set([item['id'] for item in result.items]).issubset(expected_ids)

    # query next page if expected
    if len(expected_ids) > limit:
        assert result.last_evaluated_key is not None
        assert result.last_evaluated_key['id'] == result.items[-1]['id']

        next_result = table.query(polygon=polygon, limit=limit, exclusive_start_key=result.last_evaluated_key)
        assert set([item['id'] for item in next_result.items]).issubset(expected_ids)
        assert set([item['id'] for item in next_result.items]).isdisjoint(set([item['id'] for item in result.items]))

    # query all results by pagination
    all_items = result.items
    while result.last_evaluated_key is not None:
        result = table.query(polygon=polygon, limit=limit, exclusive_start_key=result.last_evaluated_key)
        all_items += result.items

    assert len(all_items) == len(expected_ids)
    assert set([item['id'] for item in all_items]) == set(expected_ids)


def _is_item_in_polygon(bounding_box, item):
    lat = item['position']['latitude']
    lon = item['position']['longitude']
    return bounding_box.contains(shapely.geometry.Point(lon, lat))


def _setup_test_data(config: GeoTableConfiguration, num_lat=10, num_lon=10):
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
            pytest.insert_item(item)
            items.append(item)
    return items
