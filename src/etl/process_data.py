import polars as pl
from loguru import logger
from pathlib import Path
import gc

# Configuration
RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

def process_checkins():
    """Calculates checkin counts per business for pre-aggregation."""
    logger.info("Processing checkins for pre-aggregation...")
    
    # Lazy scan of checkins
    return (
        pl.scan_ndjson(RAW_DATA_DIR / "checkin.json")
        .with_columns([
            # Count number of timestamps in the comma-separated string
            pl.col("date").str.count_matches(",").add(1).alias("checkin_count")
        ])
        .select(["business_id", "checkin_count"])
    )

def process_businesses(checkin_df):
    """Cleans business data and joins with checkin counts."""
    logger.info("Processing businesses...")
    
    business_scan = pl.scan_ndjson(RAW_DATA_DIR / "business.json")
    
    processed = (
        business_scan
        .join(checkin_df, on="business_id", how="left")
        .with_columns([
            pl.col("checkin_count").fill_null(0),
            # Ensure categories are a list of strings
            pl.col("categories").str.split(", ")
        ])
    )
    
    # Using collect() here because business data is small (~150k rows)
    processed.collect().write_parquet(PROCESSED_DATA_DIR / "businesses.parquet")
    logger.success("Saved businesses.parquet")

def process_users():
    """Optimizes user data with elite flags and date casting."""
    logger.info("Processing users (streaming)...")
    
    (
        pl.scan_ndjson(RAW_DATA_DIR / "user.json")
        .with_columns([
            pl.col("yelping_since").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            # Optimization: Elite flag and count
            pl.col("elite").str.split(",").alias("elite_years")
        ])
        .with_columns([
            (pl.col("elite_years").list.len() > 0).alias("is_elite_ever"),
            pl.col("elite_years").list.len().alias("elite_year_count")
        ])
        # Sink directly to parquet to save memory
        .sink_parquet(PROCESSED_DATA_DIR / "users.parquet")
    )
    logger.success("Saved users.parquet")

def process_reviews():
    """Processes the massive review dataset using streaming."""
    logger.info("Processing reviews (streaming - this may take a few minutes)...")
    
    (
        pl.scan_ndjson(RAW_DATA_DIR / "review.json")
        .with_columns([
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
        ])
        .sink_parquet(PROCESSED_DATA_DIR / "reviews.parquet")
    )
    logger.success("Saved reviews.parquet")

def process_tips():
    """Processes tips dataset."""
    logger.info("Processing tips...")
    
    (
        pl.scan_ndjson(RAW_DATA_DIR / "tip.json")
        .with_columns([
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
        ])
        .sink_parquet(PROCESSED_DATA_DIR / "tips.parquet")
    )
    logger.success("Saved tips.parquet")

if __name__ == "__main__":
    try:
        checkin_lazy = process_checkins()
        
        process_businesses(checkin_lazy)
        process_users()
        process_tips()
        process_reviews()
        
        logger.success("ETL Pipeline completed successfully!")
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")