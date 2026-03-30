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
    logger.info("Executing Query 2: Strongest Trends in Average Star Rating Over Time...")

    # ==========================================
    # PART A: CITIES TREND
    # ==========================================
    pipeline_cities_trend =[
        # 1. Group 7M reviews by business and year (Massive data reduction)
        { "$group": {
            "_id": { 
                "business_id": "$business_id", 
                "year": { "$year": "$date" } 
            },
            "sum_stars": { "$sum": "$stars" },
            "review_count": { "$sum": 1 }
        }},
        # 2. Fast lookup against the indexed businesses collection
        { "$lookup": {
            "from": "businesses",
            "localField": "_id.business_id",
            "foreignField": "_id",
            "as": "business"
        }},
        { "$unwind": "$business" },
        # 3. Re-group by city and year
        { "$group": {
            "_id": { 
                "city": "$business.city", 
                "year": "$_id.year" 
            },
            "total_stars": { "$sum": "$sum_stars" },
            "total_reviews": { "$sum": "$review_count" }
        }},
        # 4. Calculate actual yearly average
        { "$project": {
            "city": "$_id.city",
            "year": "$_id.year",
            "avg_stars": { "$divide":["$total_stars", "$total_reviews"] },
            "total_reviews": 1
        }},
        # 5. Sort by year (Critical for identifying first/last year)
        { "$sort": { "year": 1 } },
        # 6. Final grouping by city to build the timeline and extract trend
        { "$group": {
            "_id": "$city",
            "timeline": { 
                "$push": { "year": "$year", "avg_stars": "$avg_stars", "reviews": "$total_reviews" } 
            },
            "first_year_stars": { "$first": "$avg_stars" },
            "last_year_stars": { "$last": "$avg_stars" },
            "total_city_reviews": { "$sum": "$total_reviews" },
            "years_active": { "$sum": 1 }
        }},
        # 7. Calculate trend (last_year - first_year) and filter for significance
        { "$project": {
            "city": "$_id",
            "trend": { "$subtract": ["$last_year_stars", "$first_year_stars"] },
            "timeline": 1,
            "total_city_reviews": 1,
            "years_active": 1,
            "_id": 0
        }},
        # 8. Strict outlier filtering: Min 5 years history, 5000+ total reviews
        { "$match": {
            "years_active": { "$gte": 5 },
            "total_city_reviews": { "$gte": 5000 }
        }},
        { "$sort": { "trend": -1 } }
    ]

    start = time.time()
    city_trend_results = list(db.reviews.aggregate(pipeline_cities_trend))
    elapsed = time.time() - start
    logger.success(f"Query 2 (Cities Trend) executed in {elapsed:.3f} seconds.")

    strongest_upward_cities = city_trend_results[:5]
    strongest_downward_cities = city_trend_results[-5:][::-1]

    formatted_cities = ["STRONGEST UPWARD TREND (CITIES):"] + strongest_upward_cities +["\nSTRONGEST DOWNWARD TREND (CITIES):"] + strongest_downward_cities
    save_result_to_file("2A", "Strongest Upward/Downward Trends (Cities)", pipeline_cities_trend, formatted_cities)

    # ==========================================
    # PART B: CATEGORIES TREND
    # ==========================================
    pipeline_categories_trend =[
        # 1. Group reviews by business and year
        { "$group": {
            "_id": { "business_id": "$business_id", "year": { "$year": "$date" } },
            "sum_stars": { "$sum": "$stars" },
            "review_count": { "$sum": 1 }
        }},
        # 2. Lookup business
        { "$lookup": {
            "from": "businesses",
            "localField": "_id.business_id",
            "foreignField": "_id",
            "as": "business"
        }},
        { "$unwind": "$business" },
        # 3. Unwind categories to analyze them individually
        { "$unwind": "$business.categories" },
        # 4. Re-group by category and year
        { "$group": {
            "_id": { "category": "$business.categories", "year": "$_id.year" },
            "total_stars": { "$sum": "$sum_stars" },
            "total_reviews": { "$sum": "$review_count" }
        }},
        { "$project": {
            "category": "$_id.category",
            "year": "$_id.year",
            "avg_stars": { "$divide": ["$total_stars", "$total_reviews"] },
            "total_reviews": 1
        }},
        { "$sort": { "year": 1 } },
        { "$group": {
            "_id": "$category",
            "timeline": { "$push": { "year": "$year", "avg_stars": "$avg_stars" } },
            "first_year_stars": { "$first": "$avg_stars" },
            "last_year_stars": { "$last": "$avg_stars" },
            "total_category_reviews": { "$sum": "$total_reviews" },
            "years_active": { "$sum": 1 }
        }},
        { "$project": {
            "category": "$_id",
            "trend": { "$subtract":["$last_year_stars", "$first_year_stars"] },
            "timeline": 1,
            "total_category_reviews": 1,
            "years_active": 1,
            "_id": 0
        }},
        # 8. Strict outlier filtering: Min 5 years history, 10000+ total reviews (Categories are broader)
        { "$match": {
            "years_active": { "$gte": 5 },
            "total_category_reviews": { "$gte": 10000 }
        }},
        { "$sort": { "trend": -1 } }
    ]

    start = time.time()
    cat_trend_results = list(db.reviews.aggregate(pipeline_categories_trend))
    elapsed = time.time() - start
    logger.success(f"Query 2 (Categories Trend) executed in {elapsed:.3f} seconds.")

    strongest_upward_cat = cat_trend_results[:5]
    strongest_downward_cat = cat_trend_results[-5:][::-1]

    formatted_cat = ["STRONGEST UPWARD TREND (CATEGORIES):"] + strongest_upward_cat +["\nSTRONGEST DOWNWARD TREND (CATEGORIES):"] + strongest_downward_cat
    save_result_to_file("2B", "Strongest Upward/Downward Trends (Categories)", pipeline_categories_trend, formatted_cat)

if __name__ == "__main__":
    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
        
    try:
        execute_query_1()
        execute_query_2()
        logger.info("Check queries/mongodb_answers.txt for the output.")
    except Exception as e:
        logger.exception(f"Query failed: {e}")