from utils import get_schema_from_dict
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

