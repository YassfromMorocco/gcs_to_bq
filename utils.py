import re

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