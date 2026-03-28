import polars as pl
from pymongo import MongoClient
from loguru import logger
from pathlib import Path
import math

# Config
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "yelp_db"
PROCESSED_DATA_DIR = Path("data/processed")
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger.add(LOG_DIR / "mongo_loader.log", rotation="10 MB", level="INFO")

def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

def clean_dict(d):
    """Recursively removes null/NaN values so MongoDB doesn't store garbage data."""
    clean = {}
    for k, v in d.items():
        if v is None:
            continue
        # Check for float NaN
        if isinstance(v, float) and math.isnan(v):
            continue
        clean[k] = v
    return clean

def load_collection_from_dir(dir_name: str, collection_name: str, batch_size: int = 10000):
    """
    Iterates through all partitioned parquet files in a directory 
    and streams them into MongoDB to maintain strict memory safety.
    """
    db = get_db()
    collection = db[collection_name]
    dir_path = PROCESSED_DATA_DIR / dir_name
    
    if not dir_path.exists() or not dir_path.is_dir():
        logger.error(f"Directory {dir_name} not found!")
        return

    logger.info(f"Loading partitions from {dir_name} into {collection_name}...")
    
    # Sort files to maintain chronological order if any
    parquet_files = sorted(dir_path.glob("*.parquet"))
    total_files = len(parquet_files)
    
    for idx, file_path in enumerate(parquet_files, 1):
        logger.info(f"Processing {file_path.name} ({idx}/{total_files})...")
        
        # Read the 100k row partition
        df = pl.read_parquet(file_path)
        dicts = df.to_dicts()
        
        # Process in smaller DB insertion batches (10k)
        total_rows = len(dicts)
        for i in range(0, total_rows, batch_size):
            batch = dicts[i:i + batch_size]
            
            cleaned_batch =[]
            for doc in batch:
                cleaned_doc = clean_dict(doc)
                
                # Map Yelp ID to MongoDB _id for primary indexing
                if 'business_id' in cleaned_doc:
                    cleaned_doc['_id'] = cleaned_doc.pop('business_id')
                elif 'user_id' in cleaned_doc:
                    cleaned_doc['_id'] = cleaned_doc.pop('user_id')
                elif 'review_id' in cleaned_doc:
                    cleaned_doc['_id'] = cleaned_doc.pop('review_id')
                    
                cleaned_batch.append(cleaned_doc)
            
            if cleaned_batch:
                collection.insert_many(cleaned_batch, ordered=False)
                
        logger.info(f"Finished inserting {file_path.name}")

def create_indexes():
    """Creates the indexes defined in the schema design."""
    db = get_db()
    logger.info("Creating indexes for high-speed querying...")
    
    # Businesses: Compound Index for Query 1
    db.businesses.create_index([("city", 1), ("stars", -1)])
    
    # Reviews: Date index for Query 2 & IDs for joins
    db.reviews.create_index([("date", 1)])
    db.reviews.create_index([("business_id", 1)])
    db.reviews.create_index([("user_id", 1)])
    
    # Users: Elite flag for Query 6
    db.users.create_index([("is_elite_ever", 1)])
    
    logger.success("Indexes created successfully.")

if __name__ == "__main__":
    try:
        client = MongoClient(MONGO_URI)
        logger.info("Dropping existing database to ensure clean load...")
        client.drop_database(DB_NAME)
        
        load_collection_from_dir("businesses", "businesses")
        load_collection_from_dir("users", "users")
        load_collection_from_dir("tips", "tips")
        load_collection_from_dir("reviews", "reviews")
        
        create_indexes()
        
        logger.success("MongoDB Ingestion Complete!")
    except Exception as e:
        logger.exception(f"MongoDB Loading failed: {e}")