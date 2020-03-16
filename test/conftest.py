import boto3
import pytest
from moto import mock_dynamodb2

TABLE_NAME = 'table'


def get_all_items():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)
    return table.scan()['Items']


def insert_item(item):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item=item)


def pytest_configure():
    pytest.TABLE_NAME = TABLE_NAME
    pytest.get_all_items = get_all_items
    pytest.insert_item = insert_item


@pytest.fixture()
def setup_dynamodb(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'eu-west-1')
    with mock_dynamodb2():
        dynamodb = boto3.resource('dynamodb')
        dynamodb.create_table(
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'},
                {'AttributeName': '_geohash_prefix', 'AttributeType': 'S'},
                {'AttributeName': '_geohash', 'AttributeType': 'S'},
            ],
            TableName=TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},
            ],
            GlobalSecondaryIndexes=[{
                'IndexName': 'geohash',
                'KeySchema': [
                    {'AttributeName': '_geohash_prefix', 'KeyType': 'HASH'},
                    {'AttributeName': '_geohash', 'KeyType': 'RANGE'},
                ],
                'Projection': {
                    'ProjectionType': 'ALL',
                },
            }],
            BillingMode='PAY_PER_REQUEST',
        )
        yield
