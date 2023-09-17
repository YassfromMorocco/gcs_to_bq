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


def get_schema_from_dict(file_name: str, bucket_name: str):
    """Function to get the schema from the file name"""
    print(f" FileName to get the schema from {file_name}")
    for key in source_keys:
        if re.match(key, file_name):
            schema_name = key
            print(f" Schema {schema_name} found for the file {file_name}")
