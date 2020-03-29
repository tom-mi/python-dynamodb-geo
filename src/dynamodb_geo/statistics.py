import logging
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.types import TypeDeserializer
from dynamodb_geo import GeoTableConfiguration
from dynamodb_geo.configuration import StatisticsConfiguration


class StatisticsStreamHandler:
    def __init__(self,
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
        self._statistics_table = dynamo_resource.Table(statistics_table_name)
        self._statistics_table_name = statistics_table_name
        self._client = dynamo_client
        self._source_config = source_config
        self._statistics_config: StatisticsConfiguration = statistics_config

    def handle_event(self, event):
        logging.info(f'Handling {len(event["Records"])} dynamodb stream records')
        for record in event['Records']:
            self._handle_dynamodb_stream_record(record)

    def _handle_dynamodb_stream_record(self, record):
        updates = []
        conditional_deletes = []
        if record['eventName'] == 'INSERT':
            new_geohash = self._get_geohash(record['dynamodb']['NewImage'])
            for precision in self._statistics_config.precision_steps:
                updates.append(self._get_change_item(geohash=new_geohash, precision=precision, increment=1))
        elif record['eventName'] == 'MODIFY':
            old_geohash = self._get_geohash(record['dynamodb']['OldImage'])
            new_geohash = self._get_geohash(record['dynamodb']['NewImage'])
            for precision in self._statistics_config.precision_steps:
                if old_geohash[0:precision] != new_geohash[0:precision]:
                    updates.append(self._get_change_item(geohash=old_geohash, precision=precision, increment=-1))
                    updates.append(self._get_change_item(geohash=new_geohash, precision=precision, increment=1))
                    conditional_deletes.append(self._get_conditional_delete(geohash=old_geohash, precision=precision))
        elif record['eventName'] == 'REMOVE':
            old_geohash = self._get_geohash(record['dynamodb']['OldImage'])
            for precision in self._statistics_config.precision_steps:
                updates.append(self._get_change_item(geohash=old_geohash, precision=precision, increment=-1))
                conditional_deletes.append(self._get_conditional_delete(geohash=old_geohash, precision=precision))

        if len(updates) > 0:
            self._client.transact_write_items(
                TransactItems=[{'Update': update} for update in updates],
                ClientRequestToken=f'eventId={record["eventId"]}'
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
        if precision < self._statistics_config.prefix_length:
            raise ValueError(f'Cannot create key with precision={precision} '
                             f'smaller than prefix_length={self._statistics_config.prefix_length}')
        return {
            '_geohash_prefix': {
                'S': geohash[0:self._statistics_config.prefix_length],
            },
            '_geohash': {
                'S': geohash[0:precision],
            },
        }

    def _get_geohash(self, item):
        return TypeDeserializer().deserialize(item[self._source_config.geohash_field])


def _timestamp_to_db(timestamp: datetime) -> str:
    print(timestamp)
    print(timestamp.tzinfo)
    assert timestamp.tzinfo == timezone.utc
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


# for testing
def _get_utcnow():
    return datetime.utcnow()
