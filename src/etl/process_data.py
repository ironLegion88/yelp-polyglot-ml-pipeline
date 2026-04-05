import polars as pl
from loguru import logger
from pathlib import Path
import json
from collections import Counter
from datetime import datetime

# Configuration
RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

logger.add(LOG_DIR / "etl_pipeline.log", rotation="10 MB", level="INFO")

BATCH_SIZE = 100_000

# Global Batch functions

def process_in_batches(file_path: Path, output_dir: Path, process_func, batch_size=BATCH_SIZE):
    """
    Reads a JSON file line by line in strict batches, processes them, 
    and writes to partitioned Parquet files to guarantee no OOM crashes.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return

    logger.info(f"Starting batched processing for {file_path.name}...")
    
    batch =[]
    batch_index = 0
    total_processed = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            batch.append(line)
            
            if len(batch) >= batch_size:
                _process_and_save_batch(batch, batch_index, output_dir, process_func)
                total_processed += len(batch)
                batch =[]
                batch_index += 1
                
        # Process remaining lines
        if batch:
            _process_and_save_batch(batch, batch_index, output_dir, process_func)
            total_processed += len(batch)

    logger.success(f"Finished {file_path.name}. Total processed: {total_processed}")

def _process_and_save_batch(batch_lines, batch_index, output_dir, process_func):
    """Helper to convert JSON strings to DataFrame, apply logic, and save."""
    # Read raw JSON strings into Polars
    # Using json.loads to ensure strict JSON parsing before passing to Polars
    dicts = [json.loads(line) for line in batch_lines]
    df = pl.DataFrame(dicts)
    
    # Apply specific transformations
    df_processed = process_func(df)
    
    # Save as partitioned parquet part
    output_file = output_dir / f"part_{batch_index:04d}.parquet"
    df_processed.write_parquet(output_file)
    logger.info(f"Saved {output_file.name} ({len(df_processed)} rows)")

# Document functions

def calculate_checkin_stats(df: pl.DataFrame) -> pl.DataFrame:
    """
    Parses checkin strings to calculate total counts and top 3 busiest timeframes.
    """
    results =[]
    
    for row in df.iter_rows(named=True):
        b_id = row['business_id']
        date_str = row['date']
        
        if not date_str:
            results.append({
                "business_id": b_id, "checkin_count": 0,
                "top_days":[], "top_hours": [], "top_day_hours":[]
            })
            continue
            
        timestamps = date_str.split(", ")
        
        day_counter = Counter()
        hour_counter = Counter()
        day_hour_counter = Counter()
        
        for ts in timestamps:
            # ts format: "YYYY-MM-DD HH:MM:SS"
            try:
                # Fast string slicing for hour
                hour = ts[11:13]
                # Parse date to get weekday (0=Mon, 6=Sun)
                dt_obj = datetime.fromisoformat(ts[:10])
                day = dt_obj.strftime("%A") # e.g., "Monday"
                
                day_counter[day] += 1
                hour_counter[hour] += 1
                day_hour_counter[f"{day}_{hour}:00"] += 1
            except Exception:
                continue
                
        results.append({
            "business_id": b_id,
            "checkin_count": len(timestamps),
            "top_days": [x[0] for x in day_counter.most_common(3)],
            "top_hours": [x[0] for x in hour_counter.most_common(3)],
            "top_day_hours": [x[0] for x in day_hour_counter.most_common(3)]
        })
        
    return pl.DataFrame(results)

def transform_businesses(df: pl.DataFrame, checkin_df: pl.DataFrame) -> pl.DataFrame:
    """Cleans businesses and joins with the pre-calculated checkin stats."""
    # Ensure categories is a list, not a single string
    if "categories" in df.columns and df["categories"].dtype == pl.Utf8:
        df = df.with_columns(pl.col("categories").str.split(", "))
        
    # Join with checkin stats
    joined = df.join(checkin_df, on="business_id", how="left").with_columns([
        pl.col("checkin_count").fill_null(0)
    ])
    return joined

def transform_users(df: pl.DataFrame) -> pl.DataFrame:
    """Adds elite flags, converts dates, and correctly types the friends/elite arrays."""
    
    # FIX: Correctly handle empty 'elite' strings so length is 0
    if "elite" in df.columns and df["elite"].dtype == pl.Utf8:
        df = df.with_columns(
            pl.when(pl.col("elite") == "")
            .then(None)
            .otherwise(pl.col("elite").str.split(","))
            .alias("elite_years") # Name it elite_years directly to avoid Polars crash due to target column name already existing
        ).with_columns(pl.col("elite_years").fill_null([]))
            
    # FIX: Safely parse the friends string into a List[str]
    if "friends" in df.columns and df["friends"].dtype == pl.Utf8:
        df = df.with_columns([
            pl.when(pl.col("friends").is_in(["None", ""]))
            .then(None)
            .otherwise(pl.col("friends").str.split(", "))
            .alias("friends") # Overwrite original
        ]).with_columns(pl.col("friends").fill_null([]))
        
    # Final cleanup and flag generation
    df = df.with_columns([
        pl.col("yelping_since").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
    ]).with_columns([
        (pl.col("elite_years").list.len() > 0).alias("is_elite_ever"),
        (pl.col("elite_years").list.len()).alias("elite_year_count")
    ])
    
    # Drop the original elite string column if it's still there
    if "elite" in df.columns:
        df = df.drop("elite")
        
    return df

def transform_reviews(df: pl.DataFrame) -> pl.DataFrame:
    """Converts review dates to datetime."""
    return df.with_columns([
        pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
    ])

def transform_tips(df: pl.DataFrame) -> pl.DataFrame:
    """Converts tip dates to datetime."""
    return df.with_columns([
        pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
    ])

# main function

if __name__ == "__main__":
    try:
        logger.info("Starting memory-optimized ETL pipeline...")

        # 1. Process Checkins completely first
        checkin_path = RAW_DATA_DIR / "checkin.json"
        if checkin_path.exists():
            logger.info("Pre-calculating checkin statistics...")
            # Safe to load checkins entirely as it's small
            checkin_raw = pl.DataFrame([json.loads(line) for line in open(checkin_path, 'r', encoding='utf-8')])
            global_checkin_df = calculate_checkin_stats(checkin_raw)
        else:
            global_checkin_df = pl.DataFrame({"business_id":[], "checkin_count":[]})

        # 2. Process Businesses (injecting the checkin stats)
        process_in_batches(
            RAW_DATA_DIR / "business.json", 
            PROCESSED_DATA_DIR / "businesses", 
            lambda df: transform_businesses(df, global_checkin_df)
        )

        # 3. Process Users
        process_in_batches(
            RAW_DATA_DIR / "user.json", 
            PROCESSED_DATA_DIR / "users", 
            transform_users
        )

        # 4. Process Tips
        process_in_batches(
            RAW_DATA_DIR / "tip.json", 
            PROCESSED_DATA_DIR / "tips", 
            transform_tips
        )

        # 5. Process Reviews (~5GB)
        process_in_batches(
            RAW_DATA_DIR / "review.json", 
            PROCESSED_DATA_DIR / "reviews", 
            transform_reviews
        )

        logger.success("ETL Pipeline completed successfully! All data written to partitioned Parquet directories.")
        
    except Exception as e:
        logger.exception(f"ETL Pipeline failed: {e}")