from utils import (get_schema_from_dict,
                   get_table_name_from_dict,
                   get_dataset_name_from_table,
                   generate_table_id,
                   refresh_bq_table,
                   create_bq_job_config,
                   write_to_bq_using_uri)
import os


def run_cockpit_sfr_data_ingestion(path_name: str, bucket: str):
    print(" ETL cockpit_sft triggered")
    file_name, file_extension = os.path.splitext(path_name.split("/")[-1])
    print(f" bucket is {bucket}")
    print(f" file name is {file_name}")
    print(f" file extension is {file_extension}")
    
    schema_name, schema = get_schema_from_dict(file_name=file_name,
                                               bucket_name=bucket,
                                               path_name=path_name)
    print(f" Schema {schema_name} found for the file {file_name}")
    print(f" Schema fileds are {schema} for table {file_name}")
    
    table_name = get_table_name_from_dict(f"{schema_name}")
    print(f"Table name is  {table_name}")

    dataset_name = get_dataset_name_from_table(table_name)
    print(f"datset name is {dataset_name}")

    table_id = generate_table_id(table_name, dataset_name)
    print(f"table_id is {table_id}")

    refresh_bq_table(table_id=table_id, schema=schema)
    
    job_config = create_bq_job_config(file_name)
    print(f"job config is {job_config}")
    
    write_to_bq_using_uri(path_name=file_name, bucket=bucket,
                          table=table_id, job_config=job_config,
                          file_extension=file_extension)