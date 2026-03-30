import time
import json
from pymongo import MongoClient
from loguru import logger
from pathlib import Path

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "yelp_db"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(LOG_DIR / "mongo_queries.log", rotation="10 MB", level="INFO")

OUTPUT_DIR = Path("queries")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "mongodb_answers.txt"

def get_db():
    return MongoClient(MONGO_URI)[DB_NAME]

def format_pipeline(pipeline):
    """Formats the MongoDB aggregation pipeline to a readable JSON string for the report."""
    return json.dumps(pipeline, indent=2)

def save_result_to_file(question_num, title, pipeline, results):
    """Appends the query and results to the submission text file."""
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(f"{'='*80}\n")
        f.write(f"Query {question_num}: {title}\n")
        f.write(f"{'='*80}\n\n")
        f.write("PIPELINE (The Query):\n")
        f.write(format_pipeline(pipeline) + "\n\n")
        f.write("RESULTS:\n")
        for idx, res in enumerate(results, 1):
            f.write(f"  {idx}. {res}\n")
        f.write("\n\n")

def execute_query_1():
    db = get_db()
    logger.info("Executing Query 1: Safest and Least-Safe Cities and Categories...")

    # ==========================================
    # PART A: CITIES
    # ==========================================
    # Threshold of at least 50 businesses and 1000 reviews to account for volume.
    pipeline_cities =[
        { "$group": {
            "_id": "$city",
            "avg_stars": { "$avg": "$stars" },
            "total_reviews": { "$sum": "$review_count" },
            "business_count": { "$sum": 1 }
        }},
        { "$match": { 
            "total_reviews": { "$gte": 1000 },
            "business_count": { "$gte": 50 } 
        }},
        { "$sort": { "avg_stars": -1 } }
    ]

    start = time.time()
    city_results = list(db.businesses.aggregate(pipeline_cities))
    elapsed = time.time() - start
    logger.success(f"Query 1 (Cities) executed in {elapsed:.3f} seconds.")

    safest_cities = city_results[:5]
    least_safe_cities = city_results[-5:][::-1] # Reverse to get lowest first

    formatted_cities = ["SAFEST CITIES:"] + safest_cities +["\nLEAST-SAFE CITIES:"] + least_safe_cities
    save_result_to_file("1A", "Safest and Least-Safe Cities (Min 50 businesses, 1000 reviews)", pipeline_cities, formatted_cities)

    # ==========================================
    # PART B: CATEGORIES
    # ==========================================
    # We unwind the embedded categories array. Since the array is bounded (small), this is extremely fast in memory. 
    # Higher thresholds because categories are broader than cities.
    pipeline_categories =[
        { "$unwind": "$categories" },
        { "$group": {
            "_id": "$categories",
            "avg_stars": { "$avg": "$stars" },
            "total_reviews": { "$sum": "$review_count" },
            "business_count": { "$sum": 1 }
        }},
        { "$match": { 
            "total_reviews": { "$gte": 5000 },
            "business_count": { "$gte": 100 } 
        }},
        { "$sort": { "avg_stars": -1 } }
    ]

    start = time.time()
    category_results = list(db.businesses.aggregate(pipeline_categories))
    elapsed = time.time() - start
    logger.success(f"Query 1 (Categories) executed in {elapsed:.3f} seconds.")

    safest_categories = category_results[:5]
    least_safe_categories = category_results[-5:][::-1]

    formatted_categories = ["SAFEST CATEGORIES:"] + safest_categories +["\nLEAST-SAFE CATEGORIES:"] + least_safe_categories
    save_result_to_file("1B", "Safest and Least-Safe Business Categories (Min 100 businesses, 5000 reviews)", pipeline_categories, formatted_categories)

if __name__ == "__main__":
    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
        
    try:
        execute_query_1()
        logger.info("Check queries/mongodb_answers.txt for the output.")
    except Exception as e:
        logger.exception(f"Query failed: {e}")