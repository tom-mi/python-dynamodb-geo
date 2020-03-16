from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Optional

from shapely.geometry import Point


@dataclass(frozen=True)
class GeoPosition:
    latitude: Decimal
    longitude: Decimal

    def to_shapely_point(self) -> Point:
        return Point(float(self.longitude), float(self.latitude))  # lon, lat == x, y


@dataclass(frozen=True)
class GeoBoundingBox:
    left: float
    bottom: float
    right: float
    top: float


@dataclass(frozen=True)
class QueryResult:
    items: List[Dict]
    last_evaluated_key: Optional[Dict]
