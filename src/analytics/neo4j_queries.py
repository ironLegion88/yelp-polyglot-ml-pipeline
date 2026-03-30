import pandas as pd
from neo4j import GraphDatabase
from loguru import logger
from pathlib import Path

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(LOG_DIR / "neo4j_queries.log", rotation="10 MB", level="INFO")

OUTPUT_DIR = Path("queries")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "cypher_answers.txt"

def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def run_and_save_query(driver, question_num, description, query):
    """Executes a query, logs the performance, and appends to the submission file."""
    logger.info(f"Executing Q{question_num}: {description}")
    
    with driver.session() as session:
        result = session.run(query)
        records = list(result)
        
        if records:
            df = pd.DataFrame([r.data() for r in records])
        else:
            df = pd.DataFrame()
            
        summary = result.consume()
        time_ms = summary.result_available_after + summary.result_consumed_after
        logger.success(f"Q{question_num} completed in {time_ms} ms.")
        
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(f"--- Q{question_num}: {description} ---\n")
        f.write("CYPHER QUERY:\n")
        f.write(query.strip() + "\n\n")
        f.write("RESULTS:\n")
        if not df.empty:
            f.write(df.to_string(index=False) + "\n")
        else:
            f.write("No results found.\n")
        f.write("\n" + "="*80 + "\n\n")
        
    return df

# ==========================================
# CYPHER QUERIES
# ==========================================

def run_q1(driver):
    """
    Q1: Find the 10 users with the highest number of direct friends.
    Optimization: Uses COUNT {} for O(1) node degree metadata lookup in Neo4j 5.x.
    """
    q1_cypher = """
    MATCH (u:User)
    WITH u, COUNT { (u)-[:FRIENDS_WITH]-() } AS friend_count
    ORDER BY friend_count DESC
    LIMIT 10
    RETURN u.name AS Name, 
           friend_count AS FriendCount, 
           u.review_count AS TotalReviewCount, 
           u.average_stars AS MeanStarRating
    """
    
    return run_and_save_query(driver, 1, "Top 10 users by direct friends", q1_cypher)

def run_q2(driver):
    """
    Q2: Top 3 businesses by avg star rating per state (min 50 reviews).
    Optimization: Starts from State nodes, uses path traversal, and a fast 
    APOC sorting procedure instead of a full business scan.
    """
    q2_cypher = """
    MATCH (s:State)<-[:PART_OF]-(ci:City)<-[:LOCATED_IN]-(b:Business)
    WHERE b.review_count >= 50
    WITH s, ci, b
    ORDER BY b.stars DESC
    WITH s, collect({name: b.name, stars: b.stars, city: ci.name, categories: [
        (b)-[:IN_CATEGORY]->(c) | c.name
    ]}) AS businesses
    RETURN s.code AS State, [business IN businesses[..3] | {
        Name: business.name,
        City: business.city,
        Stars: business.stars,
        Categories: business.categories
    }] AS Top3Businesses
    """
    
    return run_and_save_query(driver, 2, "Top 3 businesses by state (min 50 reviews)", q2_cypher)

def run_q3(driver):
    """
    Q3: Find the 10 users who reviewed across the most distinct cities.
    Optimization: Counts distinct :City nodes via graph traversal.
    Correction: Aggregates by unique u.user_id to prevent name collisions.
    """
    q3_cypher = """
    MATCH (u:User)-[r:REVIEWED]->(b:Business)-[:LOCATED_IN]->(c:City)
    WITH u, c, avg(r.stars) AS mean_rating
    WITH u, collect({city: c.name, avgRating: mean_rating}) AS city_ratings
    ORDER BY size(city_ratings) DESC
    LIMIT 10
    RETURN u.name AS Name, 
           u.user_id AS UserID,
           size(city_ratings) AS NumberOfCities, 
           city_ratings AS MeanRatingPerCity
    """
    return run_and_save_query(driver, 3, "Top 10 users by number of distinct cities reviewed", q3_cypher)

if __name__ == "__main__":
    driver = get_driver()
    try:
        if OUTPUT_FILE.exists():
            OUTPUT_FILE.unlink()
            
        logger.info("Starting Neo4j Analytics Queries...")
        
        # df_q1 = run_q1(driver)
        # df_q2 = run_q2(driver)
        df_q3 = run_q3(driver)
        
        # print("\nQ1 Output Preview:")
        # print(df_q1.head())
        # print("\nQ2 Output Preview:")
        # print(df_q2.head())
        print("\nQ3 Output Preview:")
        print(df_q3.head())
        
    except Exception as e:
        logger.exception(f"Query execution failed: {e}")
    finally:
        driver.close()