from pymongo import MongoClient
from loguru import logger
import time

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "yelp_db"

def augment_database():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    logger.info("Augmenting Database for Part 2...")
    
    # 1. Initialize all tip_counts to 0 first (for businesses with no tips)
    logger.info("Initializing tip_count to 0 for all businesses...")
    db.businesses.update_many(
        {"tip_count": {"$exists": False}}, 
        {"$set": {"tip_count": 0}}
    )
    
    # 2. Use $merge to calculate tip counts and update businesses in-place
    logger.info("Calculating tip_count from tips collection and merging into businesses...")
    pipeline =[
        # Group tips by business_id and count them
        { "$group": { 
            "_id": "$business_id", 
            "tip_count": { "$sum": 1 } 
        }},
        # Merge the result directly back into the businesses collection
        { "$merge": { 
            "into": "businesses", 
            "on": "_id", 
            "whenMatched": "merge", 
            "whenNotMatched": "discard" 
        }}
    ]
    
    start = time.time()
    db.tips.aggregate(pipeline)
    elapsed = time.time() - start
    
    logger.success(f"Database augmentation complete in {elapsed:.2f} seconds!")
    logger.info("The businesses collection now natively supports tip-to-review ratio queries.")

if __name__ == "__main__":
    augment_database()