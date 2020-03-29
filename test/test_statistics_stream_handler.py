import time
from datetime import timezone, datetime
from decimal import Decimal
from typing import List, Tuple, Dict, Optional
from unittest import mock

import dynamodb_geo
import pytest
from boto3.dynamodb.types import TypeSerializer
from dynamodb_geo import GeoTableConfiguration
from dynamodb_geo.configuration import StatisticsConfiguration
from dynamodb_geo.statistics import StatisticsStreamHandler

SOURCE_CONFIG = GeoTableConfiguration(partition_key_field='id')
STATISTICS_CONFIG = StatisticsConfiguration(
    precision_steps=(3, 7),
)
GEOHASH = 'u281z7j7ppzs'
OTHER_GEOHASH = 'u281hsd54tnu'
GEOHASH_PREFIX = 'u28'
TIME = datetime(2020, 3, 29, 13, 17, 1, tzinfo=timezone.utc)
OLD_TIME_DB = '2020-03-26T13:17:01.000000Z'
TIME_DB = '2020-03-29T13:17:01.000000Z'


@pytest.fixture(autouse=True)
def mock_time(monkeypatch):
    utcnow_mock = mock.Mock(return_value=TIME)
    monkeypatch.setattr(dynamodb_geo.statistics, '_get_utcnow', utcnow_mock)


@pytest.fixture
def handler(setup_dynamodb, local_dynamodb_client, local_dynamodb_resource):
    return StatisticsStreamHandler(
        source_table_name=pytest.TABLE_NAME,
        source_config=SOURCE_CONFIG,
        statistics_table_name=pytest.STATISTICS_TABLE_NAME,
        statistics_config=STATISTICS_CONFIG,
        dynamo_client=local_dynamodb_client,
        dynamo_resource=local_dynamodb_resource,
    )


def test_create_item(handler, get_all_items):
    event = get_stream_handler_event([
        (None, {'id': 'id-1', '_geohash': GEOHASH, '_geohash_prefix': GEOHASH_PREFIX})
    ])

    handler.handle_event(event)

    items = get_all_items(pytest.STATISTICS_TABLE_NAME)
    assert len(items) == 2
    assert get_stat_item(3, 1) in items
    assert get_stat_item(7, 1) in items


def test_event_is_handled_idempotent(handler, get_all_items):
    event = get_stream_handler_event([
        (None, {'id': 'id-1', '_geohash': GEOHASH, '_geohash_prefix': GEOHASH_PREFIX})
    ])

    handler.handle_event(event)
    handler.handle_event(event)

    items = get_all_items(pytest.STATISTICS_TABLE_NAME)
    assert len(items) == 2
    assert get_stat_item(3, 1) in items
    assert get_stat_item(7, 1) in items


def test_create_item_add_to_existing_entry(handler, get_all_items, insert_item):
    event = get_stream_handler_event([
        (None, {'id': 'id-1', '_geohash': GEOHASH, '_geohash_prefix': GEOHASH_PREFIX})
    ])
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(3, 1))

    handler.handle_event(event)

    items = get_all_items(pytest.STATISTICS_TABLE_NAME)
    assert len(items) == 2
    assert get_stat_item(3, 2) in items
    assert get_stat_item(7, 1) in items


def test_update_item_same_location(handler, insert_item, get_all_items):
    event = get_stream_handler_event([
        ({'id': 'id-1', '_geohash': GEOHASH, '_geohash_prefix': GEOHASH_PREFIX},
         {'id': 'id-1', '_geohash': GEOHASH, '_geohash_prefix': GEOHASH_PREFIX})
    ])
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(3, 1, updated_at=OLD_TIME_DB))
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(7, 1, updated_at=OLD_TIME_DB))

    handler.handle_event(event)

    items = get_all_items(pytest.STATISTICS_TABLE_NAME)
    assert len(items) == 2
    assert get_stat_item(3, 1, updated_at=OLD_TIME_DB) in items
    assert get_stat_item(7, 1, updated_at=OLD_TIME_DB) in items


def test_update_item_other_location(handler, insert_item, get_all_items):
    event = get_stream_handler_event([
        ({'id': 'id-1', '_geohash': GEOHASH, '_geohash_prefix': GEOHASH_PREFIX},
         {'id': 'id-1', '_geohash': OTHER_GEOHASH, '_geohash_prefix': GEOHASH_PREFIX})
    ])
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(3, 1))
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(7, 1))

    handler.handle_event(event)

    items = get_all_items(pytest.STATISTICS_TABLE_NAME)
    assert len(items) == 2
    assert get_stat_item(3, 1, geohash=OTHER_GEOHASH) in items
    assert get_stat_item(7, 1, geohash=OTHER_GEOHASH) in items


def test_delete_item(handler, insert_item, get_all_items):
    event = get_stream_handler_event([
        ({'id': 'id-1', '_geohash': GEOHASH, '_geohash_prefix': GEOHASH_PREFIX}, None)
    ])
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(3, 2))
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(7, 1))

    handler.handle_event(event)

    items = get_all_items(pytest.STATISTICS_TABLE_NAME)
    assert len(items) == 1
    assert get_stat_item(3, 1, geohash=GEOHASH) in items


def test_handles_items_without_geohash_gracefully(handler, get_all_items, insert_item):
    event = get_stream_handler_event([
        (None, {'id': 'id-1'}),
        ({'id': 'id-2', '_geohash': GEOHASH, '_geohash_prefix': GEOHASH_PREFIX}, None),
        ({'id': 'id-3'}, {'id': 'id-3', '_geohash': OTHER_GEOHASH, '_geohash_prefix': GEOHASH_PREFIX}),
        ({'id': 'id-3', '_geohash': GEOHASH, '_geohash_prefix': GEOHASH_PREFIX}, {'id': 'id-3'}),
    ])
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(3, 2))
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(7, 2))

    handler.handle_event(event)

    items = get_all_items(pytest.STATISTICS_TABLE_NAME)
    assert len(items) == 2
    assert get_stat_item(3, 1, geohash=OTHER_GEOHASH) in items
    assert get_stat_item(7, 1, geohash=OTHER_GEOHASH) in items


def test_full_table_reprocesing(handler, insert_item, get_all_items):
    insert_item(pytest.TABLE_NAME, {'id': 'id-1', '_geohash': OTHER_GEOHASH})
    insert_item(pytest.TABLE_NAME, {'id': 'id-2', '_geohash': GEOHASH})
    insert_item(pytest.TABLE_NAME, {'id': 'id-3', '_geohash': GEOHASH})
    insert_item(pytest.TABLE_NAME, {'id': 'id-4'})
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(3, 42))
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(5, 42))
    insert_item(pytest.STATISTICS_TABLE_NAME, get_stat_item(7, 42))

    handler.reprocess_full_table()

    items = get_all_items(pytest.STATISTICS_TABLE_NAME)
    assert len(items) == 3
    assert get_stat_item(3, 3, geohash=GEOHASH) in items
    assert get_stat_item(7, 2, geohash=GEOHASH) in items
    assert get_stat_item(7, 1, geohash=OTHER_GEOHASH) in items


def get_stream_handler_event(records: List[Tuple[Optional[Dict], Optional[Dict]]]):
    return {
        'Records': [get_stream_handler_event_record(old_item, new_item) for old_item, new_item in records],
    }


def get_stream_handler_event_record(old_item, new_item):
    record = {
        'eventId': str(time.time()),
        'dynamodb': {
            'StreamViewType': 'NEW_AND_OLD_IMAGES',
        }
    }
    if old_item and new_item:
        record['eventName'] = 'MODIFY'
    elif old_item:
        record['eventName'] = 'REMOVE'
    elif new_item:
        record['eventName'] = 'INSERT'
    else:
        raise ValueError('Please specify at least one of old_item / new_item')

    if old_item:
        record['dynamodb']['OldImage'] = {key: TypeSerializer().serialize(value) for key, value in old_item.items()}
    if new_item:
        record['dynamodb']['NewImage'] = {key: TypeSerializer().serialize(value) for key, value in new_item.items()}
    return record


def get_stat_item(precision: int, count: int, geohash=GEOHASH, updated_at=TIME_DB):
    return {
        '_geohash_prefix': geohash[0:3],
        '_geohash': geohash[0:precision],
        'item_count': Decimal(count),
        'updated_at': updated_at,
    }
