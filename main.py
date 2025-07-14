import pandas as pd
import sys
import argparse
import signal
from http_client.streams.ticket_master import EventsStream, VenuesStream, AttractionsStream
from utils import filter_keys_inplace, event_columns, save_bookmark_file, read_bookmark_file, default_configuration, standardize_columns
from dotenv import load_dotenv
from logging import getLogger, basicConfig
from gcs.google_cloud import upload_dataframe_to_gcs
from os import getenv

_BOOKMARKS_PATH = "./bookmarks/"
_BASE_PATH = getenv("GOOGLE_CLOUD_BUCKET")
_EVENTS_FOLDER = 'events'

events_bookmark: str = None

load_dotenv()

basicConfig(
    level="INFO",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def bookmark_on_forced_exit(signal, frame):
    logger.info("Execution forcibly stopped. Bookmarking the last page exported.")
    if events_bookmark:
        save_bookmark_file(events_bookmark, _BOOKMARKS_PATH + "events.json")
    sys.exit(0)

def extract_events(config: dict[str, str], enable_loading: bool = False):
    event_config = config.get("events", {}).copy()
    events_bookmark = read_bookmark_file(_BOOKMARKS_PATH + "events.json", config["events"].get("startDateTime"))
    event_config["startDateTime"] = events_bookmark
    event_config["apiKey"] = config.get("apiKey")
    events_stream = EventsStream(config=event_config)
    for response in events_stream.read_pages():
        data = response.get("response").json()
        events = data.get("_embedded", {}).get("events", [])
        if events:
            for event in events:
                filter_keys_inplace(event, event_columns)
            df = pd.json_normalize(events)

            df = standardize_columns(df)

            events_bookmark = response["bookmark"]
            logger.info(f"Exported {response.get("record_count")} records.")

            if enable_loading:
                file_path = upload_dataframe_to_gcs(df,_BASE_PATH, _EVENTS_FOLDER)
                if file_path != '':
                    logger.info(f"Uploaded file to {file_path}")

    logger.info("Bookmarking events stream.")
    save_bookmark_file(events_bookmark, _BOOKMARKS_PATH + "events.json")

def main(config: dict[str, str], enable_extraction: bool = True, enable_loading: bool = True):
    if enable_extraction:

        logger.info("Downloading events.")
        extract_events(config=config, enable_loading=enable_loading)
        
        logger.info("Extraction completed.")
        logger.debug(config)

    if enable_loading:
        logger.info("Loading compleated.")

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Data loader")

    parser.add_argument(
        "--enable-extraction",
        action="store_true", 
    )

    parser.add_argument(
        "--enable-loading",
        action="store_true"
    )
    
    args = parser.parse_args()
    logger = getLogger("Main")
    signal.signal(signal.SIGINT, bookmark_on_forced_exit)

    logger.info(f"Cred path: {getenv("GOOGLE_APPLICATION_CREDENTIALS")}")

    logger.info("Initializing data extraction.")

    main(
        config=default_configuration, 
        enable_extraction=args.enable_extraction, 
        enable_loading=args.enable_loading
    )

    logger.info("Extraction and loading completed.")
