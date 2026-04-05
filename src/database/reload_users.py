import polars as pl
import json
from pymongo import MongoClient
from loguru import logger
from pathlib import Path
from src.etl.process_data import transform_users, BATCH_SIZE # Import the existing logic

RAW_USER_JSON = Path("data/raw/user.json")
PROCESSED_USER_DIR = Path("data/processed/users")
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "yelp_db"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(LOG_DIR / "reload_users.log", rotation="10 MB", level="INFO")

def reload_users():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    logger.info("Dropping corrupted 'users' collection...")
    db.users.drop()
    
    # 1. Run Corrected ETL for Users
    logger.info("Running corrected ETL for Users (Streaming)...")
    PROCESSED_USER_DIR.mkdir(parents=True, exist_ok=True)
    
    # Process in batches to ensure memory safety
    batch = []
    batch_idx = 0
    with open(RAW_USER_JSON, 'r', encoding='utf-8') as f:
        for line in f:
            batch.append(json.loads(line))
            if len(batch) >= BATCH_SIZE:
                df = transform_users(pl.DataFrame(batch))
                df.write_parquet(PROCESSED_USER_DIR / f"part_{batch_idx:04d}.parquet")
                batch = []; batch_idx += 1
        if batch:
            df = transform_users(pl.DataFrame(batch))
            df.write_parquet(PROCESSED_USER_DIR / f"part_{batch_idx:04d}.parquet")

    # 2. Re-load into MongoDB
    logger.info("Re-loading corrected data into MongoDB...")
    parquet_files = sorted(PROCESSED_USER_DIR.glob("*.parquet"))
    for file in parquet_files:
        data = pl.read_parquet(file).to_dicts()
        for doc in data:
            doc['_id'] = doc.pop('user_id')
        db.users.insert_many(data, ordered=False)
        logger.info(f"Loaded {file.name}")

    # 3. Restore Index
    logger.info("Restoring index on 'is_elite_ever'...")
    db.users.create_index([("is_elite_ever", 1)])
    logger.success("Users collection successfully restored with correct Elite and Friend types!")

if __name__ == "__main__":
    reload_users()