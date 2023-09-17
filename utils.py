import re
from config import SOURCES, PROJECT_ID
from google.cloud import bigquery, storage

source_keys = list(SOURCES.keys())

# initialise the storage client and bigquery client
STORAGE_CLIENT = storage.Client(PROJECT_ID)
BIGQUERY_CLIENT = bigquery.Client(PROJECT_ID)


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
            move_data_in_blob(
                source_bucket_name=bucket_name,
                blob_name=path_name,
                target_bucket_name=bucket_name,
                new_blob_name=f"rejected/{path_name}",
                )
    return schema_name, schema


def move_data_in_blob(source_bucket_name: str, blob_name: str,
                      target_bucket_name: str, new_blob_name: str):
    """
    Function for moving files between directories or buckets. it will use GCP's copy
    function then delete the blob from the old location.

    Parameters
    -----
    source_bucket_name: name of bucket
    blob_name: str, name of file
        ex. 'data/some_location/path_name'
    target_bucket_name: name of bucket (can be same as original if we're just moving around directories)
    new_blob_name: str, name of file in new directory in target bucket
        ex. 'data/destination/path_name'
    """
    source_bucket = STORAGE_CLIENT.bucket(source_bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = STORAGE_CLIENT.bucket(target_bucket_name)

    # copy to new destination
    source_bucket.copy_blob(source_blob, destination_bucket, new_blob_name)
    # delete in old destination
    source_blob.delete()
    print(f"File moved from {source_blob} to {new_blob_name}")