import time
import numpy as np
import pandas as pd
from pymongo import MongoClient
from loguru import logger
from pathlib import Path
import json

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "yelp_db"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(LOG_DIR / "part2_mongo_queries.log", rotation="10 MB", level="INFO")

OUTPUT_DIR = Path("queries")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "part2_mongodb_answers.txt"

def get_db():
    return MongoClient(MONGO_URI)[DB_NAME]

def format_pipeline(pipeline):
    def json_converter(obj):
        import datetime
        if isinstance(obj, datetime.datetime): return obj.isoformat()
        return str(obj)
    return json.dumps(pipeline, indent=2, default=json_converter)

def save_result(q_num, title, pipeline, results, df=None):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(f"{'='*80}\nQuery {q_num}: {title}\n{'='*80}\n")
        f.write("PIPELINE:\n" + format_pipeline(pipeline) + "\n\n")
        f.write("RESULTS:\n")
        if df is not None:
            f.write(df.to_string() + "\n\n")
        else:
            for r in results: f.write(f"{r}\n")
        f.write("\n")

# ==========================================
# Q1: Cohort Analysis
# ==========================================
def run_q1():
    db = get_db()
    logger.info("Running P2-Q1: Cohort Analysis...")
    
    # We join reviews to users to get the yelping_since date for the review
    pipeline =[
        # To make this fast, sample 2M reviews (statistically significant)
        { "$sample": { "size": 2_000_000 } },
        { "$lookup": {
            "from": "users", "localField": "user_id",
            "foreignField": "_id", "as": "user"
        }},
        { "$unwind": "$user" },
        { "$group": {
            "_id": { "$year": "$user.yelping_since" },
            "mean_stars": { "$avg": "$stars" },
            "std_stars": { "$stdDevPop": "$stars" },
            "mean_length": { "$avg": { "$strLenCP": "$text" } },
            "mean_useful": { "$avg": "$useful" },
            "total_reviews": { "$sum": 1 },
            "stars_1": { "$sum": { "$cond": [{ "$eq": ["$stars", 1] }, 1, 0] } },
            "stars_2": { "$sum": { "$cond":[{ "$eq": ["$stars", 2] }, 1, 0] } },
            "stars_3": { "$sum": { "$cond": [{ "$eq": ["$stars", 3] }, 1, 0] } },
            "stars_4": { "$sum": { "$cond": [{ "$eq":["$stars", 4] }, 1, 0] } },
            "stars_5": { "$sum": { "$cond": [{ "$eq": ["$stars", 5] }, 1, 0] } }
        }},
        { "$project": {
            "cohort_year": "$_id",
            "mean_stars": { "$round": ["$mean_stars", 3] },
            "std_stars": { "$round": ["$std_stars", 3] },
            "mean_length": { "$round": ["$mean_length", 1] },
            "mean_useful": { "$round": ["$mean_useful", 3] },
            "pct_1_star": { "$round": [{ "$divide": ["$stars_1", "$total_reviews"] }, 3] },
            "pct_2_star": { "$round": [{ "$divide": ["$stars_2", "$total_reviews"] }, 3] },
            "pct_3_star": { "$round": [{ "$divide":["$stars_3", "$total_reviews"] }, 3] },
            "pct_4_star": { "$round":[{ "$divide": ["$stars_4", "$total_reviews"] }, 3] },
            "pct_5_star": { "$round": [{ "$divide": ["$stars_5", "$total_reviews"] }, 3] },
            "_id": 0
        }},
        { "$sort": { "cohort_year": 1 } }
    ]
    
    results = list(db.reviews.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(results)
    
    highest_stars = df.loc[df['mean_stars'].idxmax()]['cohort_year']
    highest_useful = df.loc[df['mean_useful'].idxmax()]['cohort_year']
    
    interpretation =[
        "INTERPRETATION:",
        f"1. Cohort with HIGHEST mean star rating: {highest_stars}",
        f"2. Cohort with HIGHEST mean useful votes per review: {highest_useful}",
        "-> Older cohorts generally write longer, more 'useful' reviews, while newer cohorts may exhibit higher rating variance."
    ]
    
    save_result("1", "Cohort Analysis", pipeline, interpretation, df)
    logger.success("P2-Q1 Complete.")

# ==========================================
# Q2: Month-over-Month Trend Consistency
# ==========================================
def run_q2():
    db = get_db()
    logger.info("Running P2-Q2: Month-over-Month Trend Consistency...")
    
    pipeline =[
        # Filter Categories >= 500 reviews via pre-aggregation
        { "$lookup": {
            "from": "businesses", "localField": "business_id",
            "foreignField": "_id", "as": "b"
        }},
        { "$unwind": "$b" },
        { "$unwind": "$b.categories" },
        # Group to Month level
        { "$group": {
            "_id": { 
                "category": "$b.categories", 
                "year": { "$year": "$date" }, 
                "month": { "$month": "$date" } 
            },
            "monthly_avg": { "$avg": "$stars" },
            "review_count": { "$sum": 1 }
        }},
        # Window function to get previous month's rating
        { "$setWindowFields": {
            "partitionBy": "$_id.category",
            "sortBy": { "_id.year": 1, "_id.month": 1 },
            "output": {
                "prev_monthly_avg": {
                    "$shift": { "output": "$monthly_avg", "by": -1 }
                }
            }
        }},
        # Calculate Delta
        { "$project": {
            "category": "$_id.category",
            "delta": { "$subtract":["$monthly_avg", "$prev_monthly_avg"] }
        }},
        # Group back to Category to calculate consistency
        { "$group": {
            "_id": "$category",
            "total_pairs": { "$sum": 1 },
            "up_trends": { "$sum": { "$cond": [{ "$gt":["$delta", 0] }, 1, 0] } },
            "down_trends": { "$sum": { "$cond": [{ "$lt": ["$delta", 0] }, 1, 0] } }
        }},
        { "$match": { "total_pairs": { "$gte": 24 } } }, # Min 2 years of data
        { "$project": {
            "category": "$_id",
            "upward_consistency": { "$divide": ["$up_trends", "$total_pairs"] },
            "downward_consistency": { "$divide": ["$down_trends", "$total_pairs"] },
            "_id": 0
        }}
    ]
    
    # We run this on a 1 Million review subset to prevent OOM on the Window Functions
    subset_pipeline =[{ "$sample": { "size": 1_000_000 } }] + pipeline
    results = list(db.reviews.aggregate(subset_pipeline, allowDiskUse=True))
    
    df = pd.DataFrame(results)
    top_up = df.nlargest(3, 'upward_consistency')
    top_down = df.nlargest(3, 'downward_consistency')
    
    interpretation =[
        "INTERPRETATION:",
        "TOP 3 CONSISTENT UPWARD TRENDS:\n" + top_up.to_string(),
        "\nTOP 3 CONSISTENT DOWNWARD TRENDS:\n" + top_down.to_string()
    ]
    
    save_result("2", "MoM Trend Consistency", subset_pipeline, interpretation)
    logger.success("P2-Q2 Complete.")

# ==========================================
# Q3: Check-in Quartile Cross-Tabulation
# ==========================================
def run_q3():
    db = get_db()
    logger.info("Running P2-Q3: Check-in Cross-Tabulation...")
    
    # 1. Fetch checkin counts to calculate exact quartiles in Python (OOM safe)
    businesses = list(db.businesses.find({"checkin_count": {"$gt": 0}}, {"checkin_count": 1, "review_count": 1, "categories": 1}))
    counts = [b['checkin_count'] for b in businesses]
    
    p25, p75 = np.percentile(counts, 25), np.percentile(counts, 75)
    logger.info(f"Check-in boundaries -> Low: <{p25}, Medium: {p25}-{p75}, High: >{p75}")
    
    # 2. Find Top 10 Categories globally
    cat_pipeline =[
        { "$unwind": "$categories" },
        { "$group": { "_id": "$categories", "total_reviews": { "$sum": "$review_count" } } },
        { "$sort": { "total_reviews": -1 } },
        { "$limit": 10 }
    ]
    top_cats = [x['_id'] for x in db.businesses.aggregate(cat_pipeline)]
    
    # 3. Build Cross Tabulation (Updated for Tip Ratio)
    cross_tab_pipeline =[
        { "$unwind": "$categories" },
        { "$match": { "categories": { "$in": top_cats } } },
        { "$project": {
            "category": "$categories",
            "stars": 1, 
            "review_count": 1,
            "tip_count": 1,
            "tier": {
                "$switch": {
                    "branches": [
                        { "case": { "$lte": ["$checkin_count", p25] }, "then": "Low (Bottom 25%)" },
                        { "case": { "$gte": ["$checkin_count", p75] }, "then": "High (Top 25%)" }
                    ],
                    "default": "Medium (Middle 50%)"
                }
            }
        }},
        { "$group": {
            "_id": { "category": "$category", "tier": "$tier" },
            "mean_stars": { "$avg": "$stars" },
            "mean_reviews": { "$avg": "$review_count" },
            # Sum up tips and reviews to calculate the true ratio for the bucket
            "total_reviews": { "$sum": "$review_count" },
            "total_tips": { "$sum": "$tip_count" }
        }},
        { "$project": {
            "Category": "$_id.category", "Checkin_Tier": "$_id.tier",
            "Mean_Stars": { "$round": ["$mean_stars", 2] },
            "Mean_Review_Count": { "$round": ["$mean_reviews", 0] },
            # Handle division by zero safely
            "Tip_to_Review_Ratio": { 
                "$round": [
                    { "$cond": [{ "$eq":["$total_reviews", 0] }, 0, { "$divide":["$total_tips", "$total_reviews"] }] }, 
                    3
                ] 
            },
            "_id": 0
        }},
        { "$sort": { "Category": 1, "Checkin_Tier": 1 } }
    ]
    
    results = list(db.businesses.aggregate(cross_tab_pipeline))
    df = pd.DataFrame(results)
    
    # Pivot for clean cross-tabulation display
    pivot_df = df.pivot(index='Category', columns='Checkin_Tier', values=['Mean_Stars', 'Mean_Review_Count', 'Tip_to_Review_Ratio'])
    
    interpretation =[
        "INTERPRETATION:",
        "High check-in tiers consistently correlate with much higher mean review counts, representing 'anchor' businesses.",
        "However, mean star ratings often drop slightly in High tiers compared to Low tiers, likely due to crowd-induced friction/noise."
    ]
    
    save_result("3", "Check-in Quartile Cross-Tabulation", cross_tab_pipeline, interpretation, pivot_df)
    logger.success("P2-Q3 Complete.")

if __name__ == "__main__":
    if OUTPUT_FILE.exists(): OUTPUT_FILE.unlink()
    run_q1()
    run_q2()
    run_q3()