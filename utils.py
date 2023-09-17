import re
from config import SOURCES

source_keys = list(SOURCES.keys())


def needs_to_be_processed(path_name: str):
    """Python function to check if the file needs to be processed"""
    if re.search("processed", path_name):
        return "processed"
    elif re.search("rejected", path_name):
        return "rejected"
    elif re.search("complete", path_name):
        return "complete"
    else:
        return True


def get_schema_from_dict(file_name: str, bucket_name: str, path_name: str):
    """Function to get the schema from the file name"""
    print(f" FileName to get the schema from {file_name}")
    print(f" source key are {source_keys}")
    for key in source_keys:
        print(f" key is {key}; filename is {file_name}")
        if re.search(file_name, key):
            schema_name = key
            print(f" Schema {schema_name} found for the file {file_name}")
            schema = SOURCES.get(schema_name, None)
            break
        else:
            print(f"no schema found for {file_name}, moving to 'rejected/'")
    return schema_name, schema

