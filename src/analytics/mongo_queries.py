import time
import json
from pymongo import MongoClient
from loguru import logger
from pathlib import Path
import datetime

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
    def json_converter(obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return str(obj)
    return json.dumps(pipeline, indent=2, default=json_converter)

def save_result_to_file(question_num, title, pipeline, results):
    """Appends the query and results to the submission text file."""
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(f"{'='*80}\n")
        f.write(f"Query {question_num}: {title}\n")
        f.write(f"{'='*80}\n\n")
        f.write("PIPELINE (The Query):\n")
        f.write(format_pipeline(pipeline) + "\n\n")
        f.write("RESULTS:\n")
        
        counter = 1
        for res in results:
            if isinstance(res, str):
                f.write(f"{res}\n")
                counter = 1 # Reset counter for the next section
            else:
                f.write(f"  {counter}. {res}\n")
                counter += 1
        f.write("\n\n")

# ====================================================================================
# QUERY 1: SAFEST AND LEAST-SAFE CITIES AND CATEGORIES
# ====================================================================================
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


# ====================================================================================
# QUERY 2: STRONGEST TRENDS IN AVERAGE STAR RATING OVER TIME (CITIES AND CATEGORIES)
# ====================================================================================
def execute_query_2():
    db = get_db()
    logger.info("Executing Query 2: Strongest Trends (Optimized with 500k Hard Cap)...")

    start_date = datetime.datetime(2017, 1, 1)
    end_date = datetime.datetime(2021, 12, 31, 23, 59, 59)
    state_subset = "PA"
    RECORD_CAP = 500_000

    # ==========================================
    # PART A: CITIES TREND
    # ==========================================
    pipeline_cities_trend =[
        { "$match": { "state": state_subset } },
        { "$lookup": {
            "from": "reviews",
            "localField": "_id",
            "foreignField": "business_id",
            "as": "review_docs"
        }},
        { "$unwind": "$review_docs" },
        
        # HARD CAP: Limit to 500k records for near-instant execution
        { "$limit": RECORD_CAP },
        
        { "$replaceRoot": {
            "newRoot": { "$mergeObjects": [ "$review_docs", { "city": "$city" } ] }
        }},
        { "$match": { "date": { "$gte": start_date, "$lte": end_date } } },
        { "$group": {
            "_id": { "city": "$city", "year": { "$year": "$date" } },
            "total_stars": { "$sum": "$stars" },
            "review_count": { "$sum": 1 }
        }},
        # Lowered threshold to 20 for the 500k sample
        { "$match": { "review_count": { "$gte": 20 } } },
        { "$project": {
            "city": "$_id.city", "year": "$_id.year",
            "avg_stars": { "$divide": ["$total_stars", "$review_count"] },
            "review_count": 1
        }},
        { "$sort": { "year": 1 } },
        { "$group": {
            "_id": "$city",
            "timeline": { "$push": { "year": "$year", "avg_stars": "$avg_stars", "reviews": "$review_count" } },
            "first_year_stars": { "$first": "$avg_stars" },
            "last_year_stars": { "$last": "$avg_stars" },
            "years_active": { "$sum": 1 }
        }},
        { "$match": { "years_active": { "$gte": 3 } } },
        { "$project": {
            "city": "$_id",
            "trend": { "$subtract": ["$last_year_stars", "$first_year_stars"] },
            "timeline": 1, "_id": 0
        }},
        { "$sort": { "trend": -1 } }
    ]

    start = time.time()
    city_trend_results = list(db.businesses.aggregate(pipeline_cities_trend, allowDiskUse=True))
    elapsed = time.time() - start
    logger.success(f"Query 2 (Cities Trend) executed in {elapsed:.3f} seconds.")

    formatted_cities = ["STRONGEST UPWARD TREND (CITIES):"] + city_trend_results[:5] +["\nSTRONGEST DOWNWARD TREND (CITIES):"] + city_trend_results[-5:][::-1]
    save_result_to_file("2A", f"Strongest Trends ({state_subset} Cities, 500k Sample)", pipeline_cities_trend, formatted_cities)

    # ==========================================
    # PART B: CATEGORIES TREND
    # ==========================================
    pipeline_categories_trend =[
        { "$match": { "state": state_subset } },
        { "$lookup": {
            "from": "reviews",
            "localField": "_id",
            "foreignField": "business_id",
            "as": "review_docs"
        }},
        { "$unwind": "$review_docs" },
        
        { "$limit": RECORD_CAP },
        
        { "$replaceRoot": {
            "newRoot": { "$mergeObjects": [ "$review_docs", { "categories": "$categories" } ] }
        }},
        { "$match": { "date": { "$gte": start_date, "$lte": end_date } } },
        { "$unwind": "$categories" },
        { "$group": {
            "_id": { "category": "$categories", "year": { "$year": "$date" } },
            "total_stars": { "$sum": "$stars" },
            "review_count": { "$sum": 1 }
        }},
        { "$match": { "review_count": { "$gte": 50 } } },
        { "$project": {
            "category": "$_id.category", "year": "$_id.year",
            "avg_stars": { "$divide": ["$total_stars", "$review_count"] },
            "review_count": 1
        }},
        { "$sort": { "year": 1 } },
        { "$group": {
            "_id": "$category",
            "timeline": { "$push": { "year": "$year", "avg_stars": "$avg_stars", "reviews": "$review_count" } },
            "first_year_stars": { "$first": "$avg_stars" },
            "last_year_stars": { "$last": "$avg_stars" },
            "years_active": { "$sum": 1 }
        }},
        { "$match": { "years_active": { "$gte": 3 } } },
        { "$project": {
            "category": "$_id",
            "trend": { "$subtract": ["$last_year_stars", "$first_year_stars"] },
            "timeline": 1, "_id": 0
        }},
        { "$sort": { "trend": -1 } }
    ]

    start = time.time()
    cat_trend_results = list(db.businesses.aggregate(pipeline_categories_trend, allowDiskUse=True))
    elapsed = time.time() - start
    logger.success(f"Query 2 (Categories Trend) executed in {elapsed:.3f} seconds.")

    formatted_cat =["STRONGEST UPWARD TREND (CATEGORIES):"] + cat_trend_results[:5] +["\nSTRONGEST DOWNWARD TREND (CATEGORIES):"] + cat_trend_results[-5:][::-1]
    save_result_to_file("2B", f"Strongest Trends ({state_subset} Categories, 500k Sample)", pipeline_categories_trend, formatted_cat)


# ====================================================================================
# Query 3: Correlation between Review Volume and Star Ratings
# ====================================================================================
def execute_query_3():
    db = get_db()
    logger.info("Executing Query 3: Correlation between Review Volume and Star Ratings...")

    # We use $bucket to create volume tiers. 
    # This reveals 'Regression to the Mean' as volume increases.
    pipeline_correlation =[
        { "$bucket": {
            "groupBy": "$review_count",
            "boundaries": [0, 10, 50, 200, 1000, 5000],
            "default": "Enterprise (5000+)",
            "output": {
                "avg_rating": { "$avg": "$stars" },
                "std_dev": { "$stdDevPop": "$stars" },
                "business_count": { "$sum": 1 },
                "min_reviews": { "$min": "$review_count" },
                "max_reviews": { "$max": "$review_count" }
            }
        }},
        { "$project": {
            "volume_tier": {
                "$switch": {
                    "branches": [
                        { "case": { "$eq": ["$_id", 0] }, "then": "0-10 (Micro)" },
                        { "case": { "$eq": ["$_id", 10] }, "then": "11-50 (Low)" },
                        { "case": { "$eq": ["$_id", 50] }, "then": "51-200 (Mid)" },
                        { "case": { "$eq": ["$_id", 200] }, "then": "201-1000 (High)" },
                        { "case": { "$eq": ["$_id", 1000] }, "then": "1001-5000 (Very High)" }
                    ],
                    "default": "5000+ (Extreme)"
                }
            },
            "avg_rating": { "$round": ["$avg_rating", 3] },
            "rating_volatility_stddev": { "$round": ["$std_dev", 3] },
            "business_count": 1,
            "_id": 0
        }}
    ]

    start = time.time()
    results = list(db.businesses.aggregate(pipeline_correlation))
    elapsed = time.time() - start
    logger.success(f"Query 3 executed in {elapsed:.3f} seconds.")

    save_result_to_file("3", "Correlation between Volume and Ratings (Bucket Analysis)", pipeline_correlation, results)

if __name__ == "__main__":
    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
        
    try:
        # execute_query_1()
        # execute_query_2()
        execute_query_3()
        logger.info("Check queries/mongodb_answers.txt for the output.")
    except Exception as e:
        logger.exception(f"Query failed: {e}")