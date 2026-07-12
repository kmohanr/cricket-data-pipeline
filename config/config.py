import os

BUCKET_NAME = os.getenv("BUCKET_NAME", "cricket-data-kmohan")

API_URL = os.getenv(
    "API_URL",
    "https://jsonplaceholder.typicode.com/todos/1"
)

DATA_PATH = os.getenv(
    "DATA_PATH",
    "/app/data"
)
