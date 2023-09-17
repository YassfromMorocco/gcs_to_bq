
def run_cockpit_sfr_data_ingestion(path_name: str, bucket: str):
    print(f" ETL cockpit_sft triggered")
    file_name = path_name.split("/")[-1]
    print(f" bucket is {bucket}")
    print(f" file name is {file_name}")