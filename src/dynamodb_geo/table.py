import logging
import time
from typing import Dict, List, Optional

import boto3
import libgeohash
from dynamodb_geo.configuration import GeoTableConfiguration
from dynamodb_geo.enricher import GeoItemEnricher
from dynamodb_geo.model import QueryResult
from shapely.geometry import Polygon, box


class GeoTable:
    MAX_PARTITIONS_TO_QUERY = 256
    # those are just guesses as of now :)
    QUERY_OPTIMIZER_MIN_HASHES = 8
    QUERY_OPTIMIZER_MAX_HASHES = 64

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

    def query(self, limit: int, polygon: Polygon = None, geohash: str = None,
              exclusive_start_key: Dict = None) -> QueryResult:
        self._validate_parameters(polygon, geohash)
        start = time.time()

        if geohash is not None:
            if len(geohash) < self._config.prefix_length or len(geohash) > self._config.precision:
                bounds = libgeohash.bbox(geohash)
                polygon = box(bounds['w'], bounds['s'], bounds['e'], bounds['n'])
        if polygon is not None:
            geohashes = sorted(self._raster_polygon(polygon))
        else:
            geohashes = [geohash]
        query_precision = len(geohashes[0])

        if exclusive_start_key:
            result = self._table.get_item(Key=exclusive_start_key)
            if 'Item' not in result:
                raise ValueError('The item with the given exclusive_start_key does not exist.')
            last_evaluated_key = self._query_key_from_item(result['Item'])
            last_geohash = result['Item']['_geohash'][0:query_precision]
            geohashes = [geohash_to_query for geohash_to_query in geohashes if geohash_to_query >= last_geohash]
        else:
            last_evaluated_key = None
        items = []
        stat_query_count = 0
        stat_query_items = 0
        for geohash_to_query in geohashes:
            while len(items) < limit + 1:
                remaining_items = limit + 1 - len(items)
                new_items, last_evaluated_key = self._query_partition(geohash_to_query, remaining_items,
                                                                      exclusive_start_key=last_evaluated_key)
                stat_query_count += 1
                stat_query_items += len(new_items)
                if polygon is not None:
                    items += self._filter_items(new_items, polygon)
                else:
                    items += new_items
                if last_evaluated_key is None:
                    break

        if len(items) >= limit + 1:
            return_last_evaluated_key = self._primary_key_from_item(items[limit - 1])
        else:
            return_last_evaluated_key = None
        delta = time.time() - start
        logging.debug(
            f'dynamodb-geo query limit={limit} hashes={len(geohashes)} query_precision={query_precision} '
            f'queries={stat_query_count}  queried_items={stat_query_items} elapsed_seconds={delta}')
        return QueryResult(items=items[0:limit], last_evaluated_key=return_last_evaluated_key)

    @staticmethod
    def _validate_parameters(polygon: Optional[Polygon], geohash: Optional[str]):
        if polygon is None and geohash is None:
            raise ValueError('Exactly one of geohash or polygon must be specified as query parameter.')
        if polygon is not None and geohash is not None:
            raise ValueError('Cannot query by both geohash and polygon.')

    def _raster_polygon(self, polygon: Polygon) -> List[str]:
        hashes = libgeohash.polygon_to_geohash(polygon, precision=self._config.prefix_length)
        if len(hashes) > self.MAX_PARTITIONS_TO_QUERY:
            raise ValueError(f'The given polygon covers {len(hashes)} partitions. '
                             f'No more than {self.MAX_PARTITIONS_TO_QUERY} are supported. '
                             'Please use a shorter prefix length to support querying larger areas.')
        if len(hashes) > self.QUERY_OPTIMIZER_MIN_HASHES:
            return hashes
        for precision in range(self._config.prefix_length + 1, self._config.precision + 1):
            current_hashes = libgeohash.polygon_to_geohash(polygon, precision=precision)
            if len(current_hashes) <= self.QUERY_OPTIMIZER_MAX_HASHES:
                hashes = current_hashes
            else:
                break
            if len(hashes) >= self.QUERY_OPTIMIZER_MIN_HASHES:
                break
        return hashes

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

    def _query_partition(self, geohash: str, limit: int, exclusive_start_key=None):
        geohash_prefix = geohash[0:self._config.prefix_length]
        params = dict(
            TableName=self._table_name,
            IndexName=self._config.geohash_index,
            KeyConditions={
                self._config.geohash_prefix_field: {'AttributeValueList': [geohash_prefix], 'ComparisonOperator': 'EQ'},
                self._config.geohash_field: {'AttributeValueList': [geohash], 'ComparisonOperator': 'BEGINS_WITH'},
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
