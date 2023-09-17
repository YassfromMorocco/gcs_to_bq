from config import SOURCES
from utils import get_schema_from_dict

def run_cockpit_sfr_data_ingestion(path_name: str, bucket: str):
    print(f" ETL cockpit_sft triggered")
    file_name = path_name.split("/")[-1]
    print(f" bucket is {bucket}")
    print(f" file name is {file_name}")
    get_schema_from_dict(file_name=file_name, bucket_name=bucket)