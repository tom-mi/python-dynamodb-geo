from decimal import Decimal

import pytest
from dynamodb_geo.enricher import GeoItemEnricher
from dynamodb_geo.configuration import GeoTableConfiguration
from dynamodb_geo.model import GeoPosition

CONFIG = GeoTableConfiguration(
    partition_key_field='id',
    geohash_field='my-geohash',
    geohash_prefix_field='my-geohash-prefix',
    position_field='my-position',
    position_mapper=lambda x: GeoPosition(latitude=x['my-lat'], longitude=x['my-lon']),
    prefix_length=4,
    precision=10,
)

LAT = Decimal('48.137154')
LON = Decimal('11.576124')
GEOHASH = 'u281z7j7pp'
GEOHASH_PREFIX = 'u281'

ITEM = {
    'my-position': {'my-lat': LAT, 'my-lon': LON},
    'other-field': {'foo': 'bar'},
}


def test_enrich_item():
    adapter = GeoItemEnricher(CONFIG)

    enriched_item = adapter.enrich_item(ITEM)

    assert enriched_item['my-position'] == ITEM['my-position']
    assert enriched_item['other-field'] == ITEM['other-field']
    assert enriched_item['my-geohash'] == GEOHASH
    assert enriched_item['my-geohash-prefix'] == GEOHASH_PREFIX


def test_original_item_is_not_modified():
    adapter = GeoItemEnricher(CONFIG)

    adapter.enrich_item(ITEM)

    assert 'my-geohash' not in ITEM
    assert 'my-geohash-prefix' not in ITEM


def test_enrich_item_prevent_overwriting_fields():
    adapter = GeoItemEnricher(CONFIG)

    with pytest.raises(ValueError):
        adapter.enrich_item({**ITEM, 'my-geohash': 'foo'})


def test_enrich_item_overwrite_fields():
    adapter = GeoItemEnricher(CONFIG)

    enriched_item = adapter.enrich_item({**ITEM, 'my-geohash': 'foo', 'my-geohash-prefix': 'foo'},
                                        overwrite_existing=True)

    assert enriched_item['my-geohash'] == GEOHASH
    assert enriched_item['my-geohash-prefix'] == GEOHASH_PREFIX
