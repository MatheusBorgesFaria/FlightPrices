import json
from os.path import join

PATH_TO_RELEVANT_PATHS = join("..", "settings","relevant_paths.json")
PATH_TO_STRUTURED_DATA_TRANSFER = join("..", "settings","structured_data_transfer_credentials.json")


def read_json(json_path=PATH_TO_RELEVANT_PATHS):
    """Read any json."""
    with open(json_path, 'r') as file:
        return json.load(file)


def get_relevant_path(key):
    """Get any key form relevant_path json."""
    return read_json(json_path=PATH_TO_RELEVANT_PATHS).get(key)


def get_structured_data_transfer_credentials():
    """Get structured_data_transfer_credentials json."""
    return read_json(json_path=PATH_TO_STRUTURED_DATA_TRANSFER)