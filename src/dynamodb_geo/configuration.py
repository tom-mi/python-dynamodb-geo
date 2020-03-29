from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Any, Tuple, Dict, Optional

from dynamodb_geo.model import GeoPosition


def latitude_longitude_mapper(field: Dict) -> GeoPosition:
    return GeoPosition(latitude=Decimal(field['latitude']), longitude=Decimal(field['longitude']))


def lat_long_mapper(field: Dict) -> GeoPosition:
    return GeoPosition(latitude=Decimal(field['lat']), longitude=Decimal(field['long']))


@dataclass
class GeoTableConfiguration:
    partition_key_field: str
    sort_key_field: Optional[str] = None
    geohash_prefix_field: str = '_geohash_prefix'
    geohash_field: str = '_geohash'
    geohash_index: str = 'geohash'
    position_field: str = 'position'
    position_mapper: Callable[[Any], GeoPosition] = latitude_longitude_mapper
    prefix_length: int = 3
    precision: int = 12


@dataclass
class StatisticsConfiguration:
    precision_steps: Tuple[int, ...] = (3, 5, 7)
