import json
from pathlib import Path

def save_bookmark_file(timestamp: str, path: str) -> None:
    """
    Saves the latest timestamp to a JSON file.

    :param timestamp: Timestamp to save
    :param path: Path to the JSON file
    """
    TIMESTAMP_FILE = Path(path)
    if TIMESTAMP_FILE.exists():
        with open(TIMESTAMP_FILE, "r") as file:
            data = json.load(file)
    else:
        data = {}

    data = {
        "startDateTime": timestamp
    }

    with open(TIMESTAMP_FILE, "w+") as file:
        json.dump(data, file, indent=4)

def read_bookmark_file(path: str, default: str) -> str:
    """
    Loads the latest timestamp from a JSON file.

    :param path: Path to the JSON file
    :param default: String to return if the file does not exist or is empty
    :return: Latest timestamp in the file
    """
    TIMESTAMP_FILE = Path(path)
    if TIMESTAMP_FILE.exists():
        with open(TIMESTAMP_FILE, "r") as file:
            data = json.load(file)
            if data:
                return data["startDateTime"]
    return default
