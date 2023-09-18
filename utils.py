import re
from config import (PROJECT_ID, BQ_SCHEMAS_MAPPING, BQ_TABLE_MAPPING,
                    DATASET_MAPPING)
from google.cloud import storage, bigquery

schema_keys = list(BQ_SCHEMAS_MAPPING.keys())
table_keys = list(BQ_TABLE_MAPPING.keys())
data_set_keys = list(DATASET_MAPPING.keys())

storage_client = storage.Client()
bigquery_client = bigquery.Client()


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
    print(f" source key are {schema_keys}")
    for key in schema_keys:
        print(f" key is {key}; filename is {file_name}")
        if re.search(file_name, key):
            schema_name = key
            schema = BQ_SCHEMAS_MAPPING.get(schema_name, None)
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
    Function for moving files between directories or buckets. 
    it will use GCP's copy function then delete the blob from the old location.

    Parameters
    -----
    source_bucket_name: name of bucket
    blob_name: str, name of file
        ex. 'data/some_location/path_name'
    target_bucket_name: name of bucket 
    (can be same as original if we're just moving around directories)
    new_blob_name: str, name of file in new directory in target bucket
        ex. 'data/destination/path_name'
    """
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(target_bucket_name)

    # copy to new destination
    source_bucket.copy_blob(source_blob, destination_bucket, new_blob_name)
    # delete in old destination
    source_blob.delete()
    print(f"File moved from {source_blob} to {new_blob_name}")


def get_table_name_from_dict(schema_name: str):
    """Python function to create the table structure for PII tables"""
    for key in schema_keys:
        if re.search(schema_name, key):
            table_name = BQ_TABLE_MAPPING.get(schema_name, None)
            break
    return table_name


def get_dataset_name_from_table(table_name: str):
    """Python function to create the table structure for PII tables"""
    for key in schema_keys:
        if re.search(table_name, key):
            table_name = DATASET_MAPPING.get(table_name, None)
            break
    return table_name


def generate_table_id(table_name: str, dataset_name: str):
    return f"{PROJECT_ID}.{dataset_name}.{table_name}"


def refresh_bq_table(table_id: str, schema: str, exists_ok: bool = True):
    """
    A function to create a new BigQuery table if it doesn't exist.
    If exists_ok is not set, defaults to True. 
    This will mean the table will not be created if it exists, 
    but no error will be thrown

    schema = [
        bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
    ]

    """
    
    table = bigquery.Table(table_id, schema=schema)
    # Make an API request.
    table = bigquery_client.create_table(table, exists_ok=exists_ok)
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id,
                                        table.table_id)
    )


def create_bq_job_config(file_name: str = None):
    """Python function to generate the job config for bigquery,
    based on the source incoming"""
    print(f"Generating the job config for {file_name}")
    try:
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.max_bad_records = 50
        job_config.field_delimiter = ","
        return job_config
    except Exception as e:
        raise Exception(f"There was an error: {e}")


def write_to_bq_using_uri(
                            path_name: str,
                            table: str,
                            bucket: str,
                            job_config: str,
                            file_extension: str
                            ):
    """"""
    # Instantiate the bqclient used to move the data
    bqclient = bigquery.Client(PROJECT_ID)
   
    try:
        write_job = bqclient.load_table_from_uri(
            source_uris=f"gs://{bucket}/{path_name}.{file_extension}",
            destination=table, project=PROJECT_ID, job_config=job_config
        )
        write_job.result()
        return True
    
    except Exception as write_error:
        print(f"There was an issue {write_error} during write to bq step")
