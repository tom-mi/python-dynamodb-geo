import logging

import boto3
import pytest

TABLE_NAME = 'table'
STATISTICS_TABLE_NAME = 'table-statistics'


@pytest.fixture(autouse=True)
def configure_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(timestamp).6f %(level)-5s %(name)s: %(message)s',
    )
    logging.getLogger('boto3').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.INFO)


@pytest.fixture
def get_all_items(local_dynamodb_resource):
    def wrapper(table_name: str):
        table = local_dynamodb_resource.Table(table_name)
        return table.scan()['Items']

    return wrapper


@pytest.fixture
def insert_item(local_dynamodb_resource):
    def wrapper(table_name: str, item):
        table = local_dynamodb_resource.Table(table_name)
        table.put_item(Item=item)

    return wrapper


def pytest_configure():
    pytest.TABLE_NAME = TABLE_NAME
    pytest.STATISTICS_TABLE_NAME = STATISTICS_TABLE_NAME
    pytest.get_all_items = get_all_items
    pytest.insert_item = insert_item


@pytest.fixture
def local_dynamodb_client():
    return boto3.client('dynamodb', endpoint_url='http://localhost:8000/',
                        aws_access_key_id='anything',
                        aws_secret_access_key='anything',
                        region_name='local',)


@pytest.fixture
def local_dynamodb_resource():
    return boto3.resource('dynamodb', endpoint_url='http://localhost:8000/',
                          aws_access_key_id='anything',
                          aws_secret_access_key='anything',
                          region_name='local')


@pytest.fixture()
def setup_dynamodb(monkeypatch, local_dynamodb_client, local_dynamodb_resource):
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'eu-west-1')
    local_dynamodb_resource.create_table(
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
    local_dynamodb_resource.create_table(
        AttributeDefinitions=[
            {'AttributeName': '_geohash', 'AttributeType': 'S'},
        ],
        TableName=STATISTICS_TABLE_NAME,
        KeySchema=[
            {'AttributeName': '_geohash', 'KeyType': 'HASH'},
        ],
        BillingMode='PAY_PER_REQUEST',
    )
    yield
    local_dynamodb_resource.Table(TABLE_NAME).delete()
    local_dynamodb_resource.Table(STATISTICS_TABLE_NAME).delete()
