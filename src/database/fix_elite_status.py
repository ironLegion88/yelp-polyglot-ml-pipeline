from pymongo import MongoClient
from loguru import logger
from pathlib import Path

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "yelp_db"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(LOG_DIR / "fix_elite_status.log", rotation="10 MB", level="INFO")

def fix_elite_status():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    logger.info("Correcting 'is_elite_ever' flags for non-elite users...")
    
    # 1. Users with elite_years = [""] or [] are NOT elite.
    result = db.users.update_many(
        { 
            "$or": [
                { "elite_years": [""] },
                { "elite_years": [] },
                { "elite_years": "" }
            ]
        },
        { 
            "$set": { 
                "is_elite_ever": False, 
                "elite_year_count": 0,
                "elite_years": [] 
            } 
        }
    )
    
    logger.success(f"Successfully corrected elite status.")
    logger.info(f"Documents modified: {result.modified_count:,}")

if __name__ == "__main__":
    fix_elite_status()