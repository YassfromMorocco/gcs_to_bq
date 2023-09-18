import schema

PROJECT_ID = 'dbt-for-bigquery-377112'
BUCKET_NAME = 'poc_landing_zone'


BQ_SCHEMAS_MAPPING = {
    "hvdn_schema": schema.hvdn_schema,
    "test_schema": schema.test_schema
}

BQ_TABLE_MAPPING = {
    "hvdn_schema": "hvdn",
}

DATASET_MAPPING = {
    "hvdn": "00_raw_cms",
}