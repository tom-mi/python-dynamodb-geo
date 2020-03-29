import logging
import math
from datetime import datetime, timezone
from typing import List

import boto3
import libgeohash
from boto3.dynamodb.types import TypeDeserializer
from dynamodb_geo import GeoTableConfiguration
from dynamodb_geo.configuration import StatisticsConfiguration
from dynamodb_geo.model import StatisticsQueryResult, StatisticsItem, GeoBoundingBox, GeoPosition
from shapely.geometry import Polygon


class StatisticsTable:
    QUERY_OPTIMIZER_MAX_HASHES = 200
    MAX_HASHES_TO_QUERY = 1000
    MAX_NUMBER_OF_RECURSIONS = 20

    def __init__(self,
                 statistics_table_name: str,
                 statistics_config: StatisticsConfiguration,
                 dynamo_client=None,
                 dynamo_resource=None,
                 ):
        if dynamo_client is None:
            dynamo_client = boto3.client('dynamodb')
        if dynamo_resource is None:
            dynamo_resource = boto3.resource('dynamodb')
        self._statistics_table = dynamo_resource.Table(statistics_table_name)
        self._statistics_table_name = statistics_table_name
        self._client = dynamo_client
        self._statistics_config = statistics_config

    def query(self, polygon: Polygon) -> StatisticsQueryResult:
        hashes = self._raster_polygon(polygon)
        query_batch_size = 100
        number_of_batches = int((len(hashes) - 1) / query_batch_size + 1)

        items = []
        for batch in range(number_of_batches):
            result = self._client.batch_get_item(
                RequestItems={
                    self._statistics_table_name: {
                        'Keys': [self._key_from_geohash(geohash)
                                 for geohash in hashes[batch * query_batch_size:(batch + 1) * query_batch_size]]
                    }
                }
            )
            items += result['Responses'][self._statistics_table_name]
        return StatisticsQueryResult(items=[self._statistics_item_from_db(item) for item in items])

    def _key_from_geohash(self, geohash: str):
        return {
            '_geohash': {'S': geohash},
        }

    def _statistics_item_from_db(self, item) -> StatisticsItem:
        geohash = TypeDeserializer().deserialize(item['_geohash'])
        item_count = TypeDeserializer().deserialize(item['item_count'])
        bbox = libgeohash.bbox(geohash)
        lat, lon = libgeohash.decode(geohash)
        return StatisticsItem(
            geohash=geohash,
            center=GeoPosition(latitude=lat, longitude=lon),
            boundaries=GeoBoundingBox(west=bbox['w'], south=bbox['s'], east=bbox['e'], north=bbox['n']),
            item_count=item_count,
        )

    def _raster_polygon(self, polygon: Polygon) -> List[str]:
        hashes = None
        for precision in sorted(self._statistics_config.precision_steps):
            number_of_recursions = self._number_of_recursions(polygon, precision=precision)
            logging.debug(f'Rastering precision={precision}, number_of_recursions={number_of_recursions}')
            if number_of_recursions > self.MAX_NUMBER_OF_RECURSIONS:
                logging.info('Skipping rasterization as the number of recursions is too high.')
                break
            current_hashes = libgeohash.polygon_to_geohash(polygon, precision=precision)
            if hashes is None:
                if len(current_hashes) <= self.MAX_HASHES_TO_QUERY:
                    hashes = current_hashes
                else:
                    raise ValueError(
                        f'The given polygon covers {len(current_hashes)} at the lowest available precision. '
                        f'No more than {self.MAX_HASHES_TO_QUERY} are supported. '
                        'Please add a lower precision_step to support querying larger areas.')
            elif len(current_hashes) <= self.QUERY_OPTIMIZER_MAX_HASHES:
                hashes = current_hashes
            else:
                break
        return hashes

    def _number_of_recursions(self, polygon: Polygon, precision: int) -> int:
        bounding_box = polygon.bounds
        diagonal_dist = libgeohash.distance((bounding_box[1], bounding_box[0]), (bounding_box[3], bounding_box[2]),
                                            coordinates=True)
        center_geohash = libgeohash.encode(polygon.centroid.y, polygon.centroid.x, precision=precision)
        center_dimensions = libgeohash.dimensions(center_geohash)
        center_geohash_diagonal_dist = math.sqrt(center_dimensions[0] ** 2 + center_dimensions[1] ** 2)
        # taken from libgeohash source to estimate computational effort
        # see https://github.com/bashhike/libgeohash/blob/master/libgeohash/geometry.py
        return int(diagonal_dist // center_geohash_diagonal_dist) + 2


class StatisticsStreamHandler:
    def __init__(self,
                 source_table_name: str,
                 source_config: GeoTableConfiguration,
                 statistics_table_name: str,
                 statistics_config: StatisticsConfiguration,
                 dynamo_client=None,
                 dynamo_resource=None,
                 ):
        if dynamo_client is None:
            dynamo_client = boto3.client('dynamodb')
        if dynamo_resource is None:
            dynamo_resource = boto3.resource('dynamodb')
        self._source_table_name = source_table_name
        self._statistics_table = dynamo_resource.Table(statistics_table_name)
        self._statistics_table_name = statistics_table_name
        self._client = dynamo_client
        self._source_config = source_config
        self._statistics_config: StatisticsConfiguration = statistics_config

    def handle_event(self, event):
        if event.get('Reprocess'):
            self.reprocess_full_table()
        elif 'Records' in event:
            logging.info(f'Handling {len(event["Records"])} dynamodb stream records')
            for record in event['Records']:
                self._handle_dynamodb_stream_record(record)
        else:
            logging.warning(f'Could not handle event {event}')

    def reprocess_full_table(self):
        logging.info('Reprocessing full table')
        paginator = self._client.get_paginator('scan')
        for page in paginator.paginate(TableName=self._statistics_table_name, ):
            for item in page['Items']:
                self._client.delete_item(TableName=self._statistics_table_name,
                                         Key={'_geohash': item['_geohash']})

        for page in paginator.paginate(TableName=self._source_table_name):
            for item in page['Items']:
                updates = []
                geohash = self._get_geohash(item)
                if geohash:
                    logging.debug(f'Reprocessing item with geohash {geohash}')
                    for precision in self._statistics_config.precision_steps:
                        updates.append(self._get_change_item(geohash=geohash, precision=precision, increment=1))
                if len(updates) > 0:
                    self._client.transact_write_items(TransactItems=[{'Update': update} for update in updates])
        logging.info('Reprocessing finished')

    def _handle_dynamodb_stream_record(self, record):
        updates = []
        conditional_deletes = []
        old_geohash, new_geohash = None, None
        if record['eventName'] == 'INSERT':
            new_geohash = self._get_geohash(record['dynamodb']['NewImage'])
        elif record['eventName'] == 'MODIFY':
            old_geohash = self._get_geohash(record['dynamodb']['OldImage'])
            new_geohash = self._get_geohash(record['dynamodb']['NewImage'])
        elif record['eventName'] == 'REMOVE':
            old_geohash = self._get_geohash(record['dynamodb']['OldImage'])

        for precision in self._statistics_config.precision_steps:
            if old_geohash and new_geohash:
                if old_geohash[0:precision] != new_geohash[0:precision]:
                    updates.append(self._get_change_item(geohash=old_geohash, precision=precision, increment=-1))
                    updates.append(self._get_change_item(geohash=new_geohash, precision=precision, increment=1))
                    conditional_deletes.append(self._get_conditional_delete(geohash=old_geohash, precision=precision))
            elif new_geohash:
                updates.append(self._get_change_item(geohash=new_geohash, precision=precision, increment=1))
            elif old_geohash:
                updates.append(self._get_change_item(geohash=old_geohash, precision=precision, increment=-1))
                conditional_deletes.append(self._get_conditional_delete(geohash=old_geohash, precision=precision))

        if len(updates) > 0:
            self._client.transact_write_items(
                TransactItems=[{'Update': update} for update in updates],
                ClientRequestToken=f'event_id={record["eventId"]}'
            )
        for delete in conditional_deletes:
            try:
                self._client.delete_item(**delete)
            except self._client.exceptions.ConditionalCheckFailedException:
                logging.debug('Conditional check failed, not deleting')

    def _get_change_item(self, geohash: str, precision: int, increment: int):
        return dict(
            ExpressionAttributeValues={
                ':item_count': {
                    'N': str(increment),
                },
                ':updated_at': {
                    'S': _timestamp_to_db(_get_utcnow()),
                }
            },
            Key=self._get_key(geohash=geohash, precision=precision),
            TableName=self._statistics_table_name,
            UpdateExpression='ADD item_count :item_count SET updated_at = :updated_at'
        )

    def _get_conditional_delete(self, geohash: str, precision: int):
        return dict(
            Key=self._get_key(geohash=geohash, precision=precision),
            TableName=self._statistics_table_name,
            ExpressionAttributeValues={
                ':item_count': {
                    'N': '0',
                },
            },
            ConditionExpression='item_count <= :item_count'
        )

    def _get_key(self, geohash: str, precision: int):
        if len(geohash) < precision:
            raise ValueError(f'Cannot create key with precision {precision} from too short geohash "{geohash}"')
        return {
            '_geohash': {
                'S': geohash[0:precision],
            },
        }

    def _get_geohash(self, item):
        if self._source_config.geohash_field in item:
            return TypeDeserializer().deserialize(item[self._source_config.geohash_field])


def _timestamp_to_db(timestamp: datetime) -> str:
    print(timestamp)
    print(timestamp.tzinfo)
    assert timestamp.tzinfo == timezone.utc
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


# for testing
def _get_utcnow():
    return datetime.utcnow()
