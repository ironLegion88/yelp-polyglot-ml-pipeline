from pymongo import MongoClient
from loguru import logger
from pathlib import Path

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "yelp_db"

LOG_DIR = Path("logs")
logger.add(LOG_DIR / "db_changes.log", rotation="10 MB")

def add_state_index():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    logger.info("Adding index on 'state' to businesses collection for faster geographic subsetting...")
    db.businesses.create_index([("state", 1)])
    
    logger.success("State index created successfully! This will be documented as a practical design decision.")

if __name__ == "__main__":
    add_state_index()