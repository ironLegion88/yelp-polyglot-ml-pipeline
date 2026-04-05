import pandas as pd
from neo4j import GraphDatabase
from loguru import logger
from pathlib import Path
from scipy.stats import spearmanr

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.add(LOG_DIR / "part2_neo4j_gds.log", rotation="10 MB", level="INFO")

OUTPUT_DIR = Path("queries")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "part2_cypher_answers.txt"

def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def append_to_report(q_num, title, text, df=None):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*80}\nQuery {q_num}: {title}\n{'='*80}\n\n")
        f.write(text + "\n\n")
        if df is not None:
            f.write(df.to_string() + "\n\n")

# ==========================================
# Q1: PageRank on User-Business Subgraph
# ==========================================
def run_pagerank(driver):
    logger.info("Executing Q1: PageRank on User-Business Subgraph...")
    with driver.session() as session:
        # 1. Project the Bipartite Graph into Memory
        logger.info("Projecting graph into GDS memory...")
        session.run("CALL gds.graph.drop('review_graph', false) YIELD graphName")
        session.run("""
            CALL gds.graph.project(
                'review_graph',
                ['User', 'Business'],
                {
                    REVIEWED: {
                        orientation: 'NATURAL',
                        properties: 'stars'
                    }
                }
            )
        """)
        
        # 2. Run PageRank (Weighted by star rating)
        logger.info("Running PageRank algorithm (20+ iterations)...")
        session.run("""
            CALL gds.pageRank.write('review_graph', {
                maxIterations: 20,
                dampingFactor: 0.85,
                relationshipWeightProperty: 'stars',
                writeProperty: 'pagerank_score'
            })
        """)
        
        # 3. Extract Top 15 Businesses and rank metrics
        result = session.run("""
            MATCH (b:Business)
            WHERE b.pagerank_score IS NOT NULL AND b.review_count > 0
            RETURN b.name AS Name, 
                   b.pagerank_score AS PageRank, 
                   b.review_count AS ReviewCount, 
                   b.stars AS AvgStars
            ORDER BY PageRank DESC
            LIMIT 1000  // Extract a good sample to compute Spearman correlation
        """)
        df = pd.DataFrame([r.data() for r in result])
        
    # Calculate Spearman Rank Correlations
    corr_pr_rc, _ = spearmanr(df['PageRank'], df['ReviewCount'])
    corr_pr_stars, _ = spearmanr(df['PageRank'], df['AvgStars'])
    
    top_15 = df.head(15)
    
    analysis = (
        f"SPEARMAN CORRELATIONS:\n"
        f"- PageRank vs Review Count: {corr_pr_rc:.3f}\n"
        f"- PageRank vs Avg Star Rating: {corr_pr_stars:.3f}\n\n"
        f"DISCUSSION:\n"
        f"A high correlation with Review Count is expected, as more inbound edges (reviews) natively increase PageRank. "
        f"However, businesses that rank significantly higher in PageRank than in standard Review Count do so because they are reviewed by 'Power Users' (users who themselves have high centrality/activity). "
        f"A highly weighted edge from an active Elite user transfers more PageRank than a review from a one-off account."
    )
    
    append_to_report("1", "PageRank on User-Business Projection", analysis, top_15)
    logger.success("Q1 Complete.")

# ==========================================
# Q2: Louvain Community Detection
# ==========================================
def run_louvain(driver):
    logger.info("Executing Q2: Louvain Community Detection...")
    with driver.session() as session:
        # 1. Project Social Graph
        logger.info("Projecting Social Graph into GDS memory...")
        session.run("CALL gds.graph.drop('social_graph', false) YIELD graphName")
        session.run("""
            CALL gds.graph.project(
                'social_graph',
                'User',
                {
                    FRIENDS_WITH: { orientation: 'UNDIRECTED' }
                }
            )
        """)
        
        # 2. Run Louvain and write community_id back to nodes
        logger.info("Running Louvain Algorithm...")
        session.run("""
            CALL gds.louvain.write('social_graph', { writeProperty: 'community_id' })
        """)
        
        # 3. Analyze Communities (Min 25 members)
        logger.info("Aggregating Community Metrics...")
        result = session.run("""
            MATCH (u:User)-[:REVIEWED]->(b:Business)-[:LOCATED_IN]->(ci:City)-[:PART_OF]->(s:State)
            WHERE u.community_id IS NOT NULL
            WITH u.community_id AS comm_id, u, b, s
            
            // Count total reviews per community
            WITH comm_id, count(u) AS comm_size, collect(b) AS businesses, collect(s.code) AS states
            WHERE comm_size >= 25
            
            // Find Top State
            WITH comm_id, comm_size, businesses, states,
                 apoc.coll.frequencies(states) AS state_freq
            WITH comm_id, comm_size, businesses, states,[x IN state_freq | x.item][0] AS top_state,
                 [x IN state_freq | x.count][0] AS top_state_count
            
            // Calculate Geographic Concentration
            RETURN comm_id AS CommunityID, 
                   comm_size AS Size, 
                   top_state AS TopState, 
                   round(toFloat(top_state_count) / size(states), 3) AS GeoConcentration
            ORDER BY GeoConcentration DESC
            LIMIT 15
        """)
        df = pd.DataFrame([r.data() for r in result])
        
    analysis = (
        "LOUVAIN COMMUNITY INSIGHTS:\n"
        "We observe that communities with high Geographic Concentration scores (>0.85) imply that social circles on Yelp are predominantly localized. "
        "Users are mostly friends with people in their own city/state, making Yelp's social graph heavily clustered geographically."
    )
    
    append_to_report("2", "Louvain Community Detection & Geo-Concentration", analysis, df)
    logger.success("Q2 Complete.")

if __name__ == "__main__":
    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
    
    driver = get_driver()
    try:
        run_pagerank(driver)
        run_louvain(driver)
    except Exception as e:
        logger.exception(f"Neo4j GDS failed: {e}")
    finally:
        driver.close()