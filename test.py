import schema
import re

file_name = "hvdnq"

SOURCES = {
    "hvdn_schema": schema.hvdn_schema,
    "test_schema": schema.test_schema
}

source_keys = list(SOURCES.keys())

print(source_keys)


for key in source_keys:
    print(f" key is {key}, file_name is {file_name}")
    if re.search(file_name, key):
        schema_name = key
        print(f" Schema {schema_name} found for the file {file_name}")
        schema = SOURCES.get(schema_name, None)
        print(f" Schema is {schema_name} and schema is {schema}")
        break
    else:
        print(f"no schema found for {file_name}, moving to 'rejected/'")


