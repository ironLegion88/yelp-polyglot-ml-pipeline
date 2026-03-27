# Yelp Polyglot Persistence & ML Pipeline

A high-performance data engineering and analytics pipeline utilizing MongoDB (Document Store) and Neo4j (Graph Database) to process and analyze the Yelp Open Dataset. 

## Architecture
- **Data Processing**: Polars (Lazy Evaluation for memory-constrained environments)
- **Document Store**: MongoDB (Optimized with embedding/referencing strategies)
- **Graph Database**: Neo4j (Cypher querying and APOC)
- **Frontend**: Streamlit 

## Setup Instructions
1. Install Python 3.11 and Docker.
2. Install dependencies: `uv pip install -r pyproject.toml` (setup pending).
3. Place Yelp JSON files in `data/raw/`.
4. Run databases: `docker-compose up -d`.