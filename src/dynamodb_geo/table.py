import logging
from typing import Dict, List

import boto3
import libgeohash
from dynamodb_geo.configuration import GeoTableConfiguration
from dynamodb_geo.enricher import GeoItemEnricher
from dynamodb_geo.model import QueryResult
from shapely.geometry import Polygon


class GeoTable:
    MAX_PARTITIONS_TO_QUERY = 128

    def __init__(self, table_name: str, config: GeoTableConfiguration, dynamo_client=None, dynamo_resource=None):
        if dynamo_client is None:
            dynamo_client = boto3.client('dynamodb')
        if dynamo_resource is None:
            dynamo_resource = boto3.resource('dynamodb')
        self._table = dynamo_resource.Table(table_name)
        self._client = dynamo_client
        self._table_name = table_name
        self._config = config
        self._enricher = GeoItemEnricher(self._config)

    def put_item(self, item: Dict, **kwargs):
        enriched_item = self._enricher.enrich_item(item=item)
        self._table.put_item(Item=enriched_item, **kwargs)

    def query(self, polygon: Polygon, limit: int, exclusive_start_key: Dict = None) -> QueryResult:

        prefix_hashes = libgeohash.polygon_to_geohash(polygon, precision=self._config.prefix_length)
        if len(prefix_hashes) > self.MAX_PARTITIONS_TO_QUERY:
            raise ValueError(f'The given polygon covers {len(prefix_hashes)} partitions. '
                             f'No more than {self.MAX_PARTITIONS_TO_QUERY} are supported. '
                             'Please use a shorter prefix length to support querying larger areas.')

        prefix_hashes = sorted(prefix_hashes)

        if exclusive_start_key:
            result = self._table.get_item(Key=exclusive_start_key)
            if 'Item' not in result:
                raise ValueError('The item with the given exclusive_start_key does not exist.')
            last_evaluated_key = self._query_key_from_item(result['Item'])
            last_prefix = result['Item']['_geohash_prefix']
            prefix_hashes = [prefix_hash for prefix_hash in prefix_hashes if prefix_hash >= last_prefix]
        else:
            last_evaluated_key = None
        items = []
        stat_query_count = 0
        stat_query_items = 0
        for prefix_hash in prefix_hashes:
            while len(items) < limit + 1:
                remaining_items = limit + 1 - len(items)
                new_items, last_evaluated_key = self._query_partition(prefix_hash, remaining_items,
                                                                      exclusive_start_key=last_evaluated_key)
                stat_query_count += 1
                stat_query_items += len(new_items)
                items += self._filter_items(new_items, polygon)
                if last_evaluated_key is None:
                    break

        logging.debug(f'dynamodb-geo query limit={limit} hashes={len(prefix_hashes)} queries={stat_query_count} '
                      f'queried_items={stat_query_items}')
        if len(items) >= limit + 1:
            return_last_evaluated_key = self._primary_key_from_item(items[limit-1])
        else:
            return_last_evaluated_key = None
        return QueryResult(items=items[0:limit], last_evaluated_key=return_last_evaluated_key)

    def _primary_key_from_item(self, item: Dict) -> Dict:
        key = {self._config.partition_key_field: item[self._config.partition_key_field]}
        if self._config.sort_key_field:
            key[self._config.sort_key_field] = item[self._config.sort_key_field]
        return key

    def _query_key_from_item(self, item: Dict) -> Dict:
        key = {
            self._config.partition_key_field: item[self._config.partition_key_field],
            self._config.geohash_prefix_field: item[self._config.geohash_prefix_field],
            self._config.geohash_field: item[self._config.geohash_field],
        }
        if self._config.sort_key_field:
            key[self._config.sort_key_field] = item[self._config.sort_key_field]
        return key

    def _query_partition(self, geohash_prefix: str, limit: int, exclusive_start_key=None):
        params = dict(
            TableName=self._table_name,
            IndexName=self._config.geohash_index,
            KeyConditions={
                self._config.geohash_prefix_field: {'AttributeValueList': [geohash_prefix], 'ComparisonOperator': 'EQ'},
            },
            Limit=limit,
        )
        if exclusive_start_key:
            params['ExclusiveStartKey'] = exclusive_start_key
        page = self._table.query(
            **params
        )
        return page['Items'], page.get('LastEvaluatedKey')

    def _filter_items(self, items: List[Dict], polygon: Polygon):
        return [item for item in items if self._is_item_in_polygon(item, polygon)]

    def _is_item_in_polygon(self, item: Dict, polygon: Polygon) -> bool:
        position_value = item[self._config.position_field]
        position = self._config.position_mapper(position_value)
        return polygon.contains(position.to_shapely_point())