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


def get_schema_from_dict(file_name: str, bucket_name: str, path_name: str = None):
    """Function to get the schema from the file name"""
    schema_to_get = file_name
    print(f" FileName to get the schema from {schema_to_get}")
    for key in source_keys:
        schema_name = key if re.search(key, schema_to_get) else None
        print(f" Schema {schema_name} found for the file {schema_to_get}")
