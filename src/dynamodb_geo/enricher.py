from copy import deepcopy
from typing import Dict

import libgeohash
from dynamodb_geo.configuration import GeoTableConfiguration


class GeoItemEnricher:

    def __init__(self, configuration: GeoTableConfiguration):
        self._config = configuration

    def enrich_item(self, item: Dict, overwrite_existing=False):
        position_value = item[self._config.position_field]
        position = self._config.position_mapper(position_value)
        geohash = libgeohash.encode(lat=float(position.latitude), lon=float(position.longitude),
                                    precision=self._config.precision)
        geohash_prefix = geohash[0:self._config.prefix_length]

        enriched_item = deepcopy(item)
        self._set_item_value(enriched_item, self._config.geohash_field, geohash, overwrite_existing)
        self._set_item_value(enriched_item, self._config.geohash_prefix_field, geohash_prefix, overwrite_existing)

        return enriched_item

    @staticmethod
    def _set_item_value(item: Dict, key: str, value: str, overwrite_existing: bool):
        if not overwrite_existing and key in item:
            raise ValueError(f'Field {key} already exists')
        item[key] = value
