import polars as pl
from neo4j import GraphDatabase
from loguru import logger
from pathlib import Path
import math

# Config
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"
PROCESSED_DATA_DIR = Path("data/processed")
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger.add(LOG_DIR / "neo4j_loader.log", rotation="10 MB", level="INFO")

def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def clean_dict_for_neo4j(d):
    """Removes NaN and formats lists safely for Neo4j."""
    clean = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, float) and math.isnan(v):
            continue
        # Ensure friends is a list if it was a comma-separated string
        if k == 'friends' and isinstance(v, str):
            clean[k] = [f.strip() for f in v.split(",") if f.strip()]
            continue
        clean[k] = v
    return clean

def create_constraints(driver):
    """Constraints enforce uniqueness AND create indexes automatically."""
    queries =[
        "CREATE CONSTRAINT IF NOT EXISTS FOR (b:Business) REQUIRE b.business_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (ci:City) REQUIRE ci.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:State) REQUIRE s.code IS UNIQUE",
    ]
    with driver.session() as session:
        for q in queries:
            session.run(q)
    logger.success("Neo4j Constraints & Indexes created.")

def run_batch_query(driver, query, batch, batch_name):
    """Executes a parameterized UNWIND query safely."""
    with driver.session() as session:
        try:
            session.run(query, batch=batch)
        except Exception as e:
            logger.error(f"Failed to execute batch in {batch_name}: {e}")

def load_graph_entities(driver, dir_name: str, query: str, batch_size: int = 5000):
    """Iterates through partitioned parquet files and executes Cypher batches."""
    dir_path = PROCESSED_DATA_DIR / dir_name
    if not dir_path.exists() or not dir_path.is_dir():
        logger.warning(f"Directory {dir_name} not found, skipping...")
        return

    logger.info(f"Loading {dir_name} into Neo4j...")
    parquet_files = sorted(dir_path.glob("*.parquet"))
    
    for file_path in parquet_files:
        df = pl.read_parquet(file_path)
        dicts = df.to_dicts()
        
        total_rows = len(dicts)
        for i in range(0, total_rows, batch_size):
            raw_batch = dicts[i:i + batch_size]
            clean_batch =[clean_dict_for_neo4j(doc) for doc in raw_batch]
            
            run_batch_query(driver, query, clean_batch, file_path.name)
            
        logger.info(f"Finished parsing {file_path.name} to Neo4j.")

def load_user_friends(driver, batch_size=10000):
    """Specially optimized loader for the dense User-Friend network."""
    dir_path = PROCESSED_DATA_DIR / "users"
    if not dir_path.exists() or not dir_path.is_dir():
        return

    logger.info("Building User-Friend network (Optimized Deduplicated Processing)...")
    parquet_files = sorted(dir_path.glob("*.parquet"))
    
    for file_path in parquet_files:
        # Read file and handle the string-to-list conversion on the fly
        df = pl.read_parquet(file_path).select(["user_id", "friends"])
        
        # Split the string if it's not already a list
        if df["friends"].dtype == pl.Utf8:
            df = df.with_columns(pl.col("friends").str.split(", "))
            
        # Explode the array into flat rows
        df = df.explode("friends").drop_nulls().rename({"friends": "friend_id"})
        
        # Filter out empty strings and "None" strings
        df = df.filter(
            (pl.col("friend_id") != "") & 
            (pl.col("friend_id") != "None")
        )
        
        # Optimization: Deduplicate undirected relationships.
        # Since Yelp friends are mutual (A has B, B has A), we enforce an alphabetical 
        # ordering condition (A < B). This cuts the number of edges we send to Neo4j 
        # exactly in half, massively saving memory and preventing duplicate processing.
        df = df.filter(pl.col("user_id") < pl.col("friend_id"))
        
        dicts = df.to_dicts()
        total_rows = len(dicts)
        
        # Process the flat pairs in strict chunks
        for i in range(0, total_rows, batch_size):
            batch = dicts[i:i + batch_size]
            run_batch_query(driver, Q_USER_FRIENDS, batch, f"{file_path.name} (Friends)")
            
        logger.info(f"Finished deduplicated friend edges for {file_path.name}")

# CYPHER QUERIES

# 1. Businesses, Cities, States, Categories
Q_BUSINESS = """
UNWIND $batch AS row
MERGE (b:Business {business_id: row.business_id})
SET b.name = row.name, b.stars = row.stars, b.review_count = row.review_count

MERGE (s:State {code: row.state})
MERGE (ci:City {name: row.city})
MERGE (ci)-[:PART_OF]->(s)
MERGE (b)-[:LOCATED_IN]->(ci)

FOREACH (cat_name IN coalesce(row.categories,[]) |
    MERGE (c:Category {name: cat_name})
    MERGE (b)-[:IN_CATEGORY]->(c)
)
"""

# 2. Users (Nodes only)
Q_USER_NODES = """
UNWIND $batch AS row
MERGE (u:User {user_id: row.user_id})
SET u.name = row.name, u.review_count = row.review_count, u.average_stars = row.average_stars
"""

# 3. User Friends (Edges)
# OPTIMIZED: Expects a flattened list of {user_id, friend_id} pairs.
# Uses MATCH to ensure we only link existing users (preventing dangling node creation).
Q_USER_FRIENDS = """
UNWIND $batch AS pair
MATCH (u:User {user_id: pair.user_id})
MATCH (f:User {user_id: pair.friend_id})
MERGE (u)-[:FRIENDS_WITH]->(f)
"""

# 4. Reviews (Edges)
Q_REVIEWS = """
UNWIND $batch AS row
MATCH (u:User {user_id: row.user_id})
MATCH (b:Business {business_id: row.business_id})
MERGE (u)-[r:REVIEWED {review_id: row.review_id}]->(b)
SET r.stars = row.stars, r.date = row.date
"""

# 5. Tips (Edges)
Q_TIPS = """
UNWIND $batch AS row
MATCH (u:User {user_id: row.user_id})
MATCH (b:Business {business_id: row.business_id})
MERGE (u)-[t:TIP_LEFT]->(b)
SET t.date = row.date
"""

if __name__ == "__main__":
    driver = get_driver()
    try:
        # Step 0: Ensure indexes exist before loading anything
        create_constraints(driver)
        
        # Step 1: Core nodes and static hierarchy
        load_graph_entities(driver, "businesses", Q_BUSINESS, batch_size=5000)
        
        # Step 2: User nodes
        load_graph_entities(driver, "users", Q_USER_NODES, batch_size=5000)
        
        # Step 3: Social edges (Optimized dense network loader)
        logger.info("Building User-Friend network...")
        load_user_friends(driver, batch_size=10000)
        
        # Step 4: Business interactions
        load_graph_entities(driver, "tips", Q_TIPS, batch_size=5000)
        load_graph_entities(driver, "reviews", Q_REVIEWS, batch_size=5000)
        
        logger.success("Neo4j Graph Building Complete!")
        
    except Exception as e:
        logger.exception(f"Neo4j Loading failed: {e}")
    finally:
        driver.close()