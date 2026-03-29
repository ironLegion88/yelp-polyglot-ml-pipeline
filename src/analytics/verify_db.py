import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from neo4j import GraphDatabase
from neo4j.time import DateTime as Neo4jDateTime
from loguru import logger
from pathlib import Path

MONGO_URI = "mongodb://localhost:27017"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"
DB_NAME = "yelp_db"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(LOG_DIR / "verify_db.log", rotation="10 MB", level="INFO")

EXPECTED_MONGO_SCHEMA = {
    "businesses": {
        "_id": "str", "name": "str", "city": "str", "stars": ("float", "int"),
        "review_count": "int", "checkin_count": "int",
        "top_days": "list", "top_hours": "list", "categories": "list"
    },
    "users": {
        "_id": "str", "name": "str", "yelping_since": "datetime",
        "is_elite_ever": "bool", "elite_year_count": "int",
        "friends": "list"
    },
    "reviews": {
        "_id": "str", "user_id": "str", "business_id": "str",
        "stars": ("int", "float"), "date": "datetime", "text": "str"
    },
    "tips": {
        "_id": "ObjectId", "user_id": "str", "business_id": "str",
        "text": "str", "date": "datetime"
    }
}

EXPECTED_NEO4J_NODES = {
    "Business": {"business_id": "str", "name": "str", "stars": ("float", "int"), "review_count": "int"},
    "User": {"user_id": "str", "name": "str", "review_count": "int", "average_stars": ("float", "int")},
    "Category": {"name": "str"},
    "City": {"name": "str"},
    "State": {"code": "str"}
}

EXPECTED_NEO4J_EDGES = {
    "REVIEWED": {"stars": ("int", "float"), "date": ("DateTime", "str")}, # Depending on driver parsing
    "TIP_LEFT": {"date": ("DateTime", "str")}
}

def get_type_name(val):
    """Helper to map Python types to string names for clean logging."""
    if isinstance(val, bool): return "bool" # Check bool before int, as bool is subclass of int
    if isinstance(val, int): return "int"
    if isinstance(val, float): return "float"
    if isinstance(val, str): return "str"
    if isinstance(val, list): return "list"
    if isinstance(val, dict): return "dict"
    if isinstance(val, datetime.datetime): return "datetime"
    if isinstance(val, ObjectId): return "ObjectId"
    if isinstance(val, Neo4jDateTime): return "DateTime"
    return type(val).__name__

def verify_mongodb():
    logger.info("=" * 60)
    logger.info("=== MONGODB VERIFICATION ===")
    logger.info("=" * 60)
    
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 1. Check Collections & Counts
    collections = db.list_collection_names()
    logger.info(f"Found Collections: {collections}")
    for coll in collections:
        count = db[coll].estimated_document_count()
        logger.info(f"Collection [{coll}] Count: {count:,}")
        
    # 2. Verify Schema Types
    logger.info("\n--- MongoDB Schema & Type Verification ---")
    for coll_name, expected_schema in EXPECTED_MONGO_SCHEMA.items():
        if coll_name not in collections:
            logger.warning(f"Collection {coll_name} is missing!")
            continue
            
        sample = db[coll_name].find_one()
        if not sample:
            logger.warning(f"Collection {coll_name} is empty!")
            continue
            
        for field, expected_type in expected_schema.items():
            if field not in sample:
                logger.error(f"  {coll_name.capitalize()} Sample: '{field}' is MISSING (Expected: {expected_type})")
                continue
                
            val = sample[field]
            actual_type = get_type_name(val)
            
            # Format val for display (truncate long strings/lists)
            display_val = repr(val)[:50] + "..." if len(repr(val)) > 50 else repr(val)
            
            # Type matching logic
            match = actual_type == expected_type if isinstance(expected_type, str) else actual_type in expected_type
            
            if match:
                logger.success(f"  {coll_name.capitalize()} Sample: {field}={display_val} (Type: {actual_type}; Expected: {expected_type})")
            else:
                logger.error(f"  {coll_name.capitalize()} Sample: {field}={display_val} (Type: {actual_type}; Expected: {expected_type}) <--- TYPE MISMATCH")

def verify_neo4j():
    logger.info("\n" + "=" * 60)
    logger.info("=== NEO4J VERIFICATION ===")
    logger.info("=" * 60)
    
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    with driver.session() as session:
        # 1. Verify Constraints
        logger.info("--- Neo4j Constraints & Indexes ---")
        # Neo4j 5.x syntax for constraints
        constraints = session.run("SHOW CONSTRAINTS YIELD labelsOrTypes, properties")
        for record in constraints:
            labels = record["labelsOrTypes"]
            props = record["properties"]
            logger.info(f"  Constraint: {labels} on {props}")
            
        # 2. Verify Node Counts
        logger.info("\n--- Neo4j Node Counts ---")
        node_counts = session.run("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count")
        for record in node_counts:
            logger.info(f"  Node [{record['label']}]: {record['count']:,}")
            
        # 3. Verify Edge Counts
        logger.info("\n--- Neo4j Relationship Counts ---")
        edge_counts = session.run("MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count")
        for record in edge_counts:
            logger.info(f"  Edge [{record['type']}]: {record['count']:,}")

        # 4. Verify Node Schema Types
        logger.info("\n--- Neo4j Node Schema & Type Verification ---")
        for label, expected_schema in EXPECTED_NEO4J_NODES.items():
            sample = session.run(f"MATCH (n:{label}) RETURN n LIMIT 1").single()    # type: ignore
            if not sample:
                logger.warning(f"Node label {label} is empty or missing!")
                continue
                
            node_props = sample["n"]
            for field, expected_type in expected_schema.items():
                if field not in node_props:
                    logger.error(f"  {label} Node Sample: '{field}' is MISSING (Expected: {expected_type})")
                    continue
                    
                val = node_props[field]
                actual_type = get_type_name(val)
                display_val = repr(val)[:50] + "..." if len(repr(val)) > 50 else repr(val)
                
                match = actual_type == expected_type if isinstance(expected_type, str) else actual_type in expected_type
                
                if match:
                    logger.success(f"  {label} Node Sample: {field}={display_val} (Type: {actual_type}; Expected: {expected_type})")
                else:
                    logger.error(f"  {label} Node Sample: {field}={display_val} (Type: {actual_type}; Expected: {expected_type}) <--- TYPE MISMATCH")

        # 5. Verify Edge Schema Types
        logger.info("\n--- Neo4j Edge Schema & Type Verification ---")
        for edge_type, expected_schema in EXPECTED_NEO4J_EDGES.items():
            sample = session.run(f"MATCH ()-[r:{edge_type}]->() RETURN r LIMIT 1").single()    # type: ignore
            if not sample:
                logger.warning(f"Edge type {edge_type} is empty or missing!")
                continue
                
            edge_props = sample["r"]
            for field, expected_type in expected_schema.items():
                if field not in edge_props:
                    logger.error(f"  {edge_type} Edge Sample: '{field}' is MISSING (Expected: {expected_type})")
                    continue
                    
                val = edge_props[field]
                actual_type = get_type_name(val)
                display_val = repr(val)[:50] + "..." if len(repr(val)) > 50 else repr(val)
                
                match = actual_type == expected_type if isinstance(expected_type, str) else actual_type in expected_type
                
                if match:
                    logger.success(f"  {edge_type} Edge Sample: {field}={display_val} (Type: {actual_type}; Expected: {expected_type})")
                else:
                    logger.error(f"  {edge_type} Edge Sample: {field}={display_val} (Type: {actual_type}; Expected: {expected_type}) <--- TYPE MISMATCH")

    driver.close()

if __name__ == "__main__":
    try:
        verify_mongodb()
        verify_neo4j()
        logger.info("\n" + "=" * 60)
        logger.success("DATABASE VERIFICATION COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
    except Exception as e:
        logger.exception(f"Verification script failed: {e}")