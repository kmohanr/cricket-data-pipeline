import json
from datetime import datetime
import os

from .api import fetch_data
from .s3_upload import upload
from .logger import logger
from config.config import BUCKET_NAME, API_URL, DATA_PATH

def main():

    logger.info("Application Started")

    data = fetch_data(API_URL)

    if data is None:
        logger.error("API Call Failed")
        return

    filename = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    filepath = os.path.join(DATA_PATH, filename)

    with open(filepath, "w") as file:
        json.dump(data, file, indent=4)

    logger.info(f"JSON Created : {filepath}")

    upload(filepath, BUCKET_NAME, filename)

    logger.info("Uploaded to S3 Successfully")

if __name__ == "__main__":
    main()
