import schema

PROJECT_ID = 'dbt-for-bigquery-377112'
BUCKET_NAME = 'poc_landing_zone'

SOURCES = {
    "hvdn_schema": schema.hvdn_schema,
    "test_schema": schema.test_schema
}
