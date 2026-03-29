from pathlib import Path
from pymongo import MongoClient
from loguru import logger

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(LOG_DIR / "fix_mongo_friends.log", rotation="10 MB", level="INFO")

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "yelp_db"

def fix_user_friends_type():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    logger.info("Starting in-place type conversion for 'friends' field in Users collection...")
    
    # Update pipeline: 
    # 1. Matches only documents where 'friends' is still a string
    # 2. If friends == "None" or "", set to empty array[]
    # 3. Else, split by ", " to create a proper array
    result = db.users.update_many(
        { "friends": { "$type": "string" } },[
            { "$set": {
                "friends": {
                    "$cond": {
                        "if": { "$in": ["$friends", ["None", ""]] },
                        "then":[],
                        "else": { "$split": ["$friends", ", "] }
                    }
                }
            }}
        ]
    )
    
    logger.success(f"Successfully converted 'friends' to arrays.")
    logger.info(f"Documents modified: {result.modified_count:,}")

if __name__ == "__main__":
    fix_user_friends_type()