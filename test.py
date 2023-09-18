import schema
from config import BQ_SCHEMAS_MAPPING
from utils import (get_table_name_from_dict,
                   get_dataset_name_from_table,
                   generate_table_id)

file_name = "hvdn"

SOURCES = {
    "hvdn_schema": schema.hvdn_schema,
    "test_schema": schema.test_schema
}

source_keys = list(SOURCES.keys())

print(source_keys)
schema_name = "hvdn_schema"
schema = BQ_SCHEMAS_MAPPING.get(schema_name, None)
print(f"schema is {schema}")

table_name = get_table_name_from_dict(schema_name)
print(f"table name is {table_name}")

dataset_name = get_dataset_name_from_table(table_name)
print(f"datset name is {dataset_name}")

table_id = generate_table_id(table_name, dataset_name)
print(f"table_id is {table_id}")