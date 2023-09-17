import functions_framework
from utils import needs_to_be_processed
from config import BUCKET_NAME
# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    
    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    
    status = needs_to_be_processed(name)
    print(f"status: {status}")
    
    if status == "processed":
        print(f" The file {name} is in the dir '/processed/'")
        return status
    
    elif status == "rejected":
        print(f" The file {name} is in the dir '/rejected/'")
        return status
    else:
        if bucket == BUCKET_NAME:
            print(f" The file {name} will be processed")