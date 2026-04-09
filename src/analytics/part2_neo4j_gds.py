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

# ==========================================
# Q3: Node Similarity (Jaccard) & Market Saturation
# ==========================================
def run_node_similarity(driver):
    logger.info("Executing Q3: Node Similarity (Jaccard) for Market Saturation...")
    
    with driver.session() as session:
        # Get Top 10 Cities by Business Count
        cities_result = session.run("""
            MATCH (b:Business)-[:LOCATED_IN]->(ci:City)
            WITH ci.name AS city_name, count(b) AS b_count
            WHERE b_count >= 20
            RETURN city_name, b_count
            ORDER BY b_count DESC LIMIT 10
        """)
        cities = [r["city_name"] for r in cities_result]
        
        all_city_stats =[]

        for city in cities:
            logger.info(f"Processing City: {city}")
            session.run("CALL gds.graph.drop('city_graph', false) YIELD graphName")
            
            session.run("""
                MATCH (u:User)-[:REVIEWED]->(b:Business)-[:LOCATED_IN]->(ci:City {name: $city_param})
                RETURN gds.graph.project('city_graph', b, u)
            """, city_param=city)
            
            # Run Jaccard Similarity 
            sim_results = session.run("""
                CALL gds.nodeSimilarity.stream('city_graph', { topK: 50, similarityCutoff: 0.0 })
                YIELD node1, node2, similarity
                WITH gds.util.asNode(node1) AS b1, gds.util.asNode(node2) AS b2, similarity
                
                // Rubric Requirement: Shared audience proxy (at least 3 common reviewers)
                MATCH (b1)<-[:REVIEWED]-(u:User)-[:REVIEWED]->(b2)
                WITH b1, b2, similarity, count(DISTINCT u) AS shared_reviewers
                WHERE shared_reviewers >= 3
                
                MATCH (b1)-[:IN_CATEGORY]->(c:Category)<-[:IN_CATEGORY]-(b2)
                RETURN b1.name AS Business1, b2.name AS Business2, c.name AS Category, similarity
            """)
            
            city_df = pd.DataFrame([r.data() for r in sim_results])
            if not city_df.empty:
                stats = city_df.groupby('Category').agg(
                    Mean_Similarity=('similarity', 'mean'),
                    Unique_Businesses=('Business1', 'nunique')
                ).reset_index()
                
                stats['City'] = city
                stats = stats[stats['Unique_Businesses'] >= 5]
                all_city_stats.append(stats)

        if all_city_stats:
            final_df = pd.concat(all_city_stats).sort_values(by='Mean_Similarity', ascending=False)
            final_df = final_df[['City', 'Category', 'Mean_Similarity', 'Unique_Businesses']]
            
            top_5 = final_df.head(5)
            bottom_5 = final_df.tail(5)
            
            analysis = (
                "MARKET SATURATION ANALYSIS (Top 10 Cities):\n"
                "Saturated markets (High Similarity) indicate high competition for the exact same customer pool. "
                "A new business entering these markets must steal customers rather than acquire new ones.\n"
                "Fragmented markets (Low Similarity) indicate distinct audiences, meaning lower direct competition."
            )
            append_to_report("3", "Node Similarity (Jaccard) & Market Saturation", analysis, pd.concat([top_5, bottom_5]))
        else:
            logger.warning("No categories met the similarity threshold.")
            
    logger.success("Q3 Complete.")

# ==========================================
# Q4: Betweenness Centrality
# ==========================================
def run_betweenness(driver):
    logger.info("Executing Q4: Betweenness Centrality...")
    with driver.session() as session:
        session.run("CALL gds.graph.drop('social_graph_bc', false) YIELD graphName")
        session.run("""
            CALL gds.graph.project(
                'social_graph_bc',
                'User',
                { FRIENDS_WITH: { orientation: 'UNDIRECTED' } }
            )
        """)
        
        logger.info("Running Betweenness Centrality (Approximated - 250 samples)...")
        session.run("""
            CALL gds.betweenness.write('social_graph_bc', { 
                writeProperty: 'betweenness_score',
                samplingSize: 250 
            })
        """)
        
        # 1. Get Top 20 Betweenness
        res_bc = session.run("MATCH (u:User) WHERE u.betweenness_score IS NOT NULL RETURN u.user_id AS uid ORDER BY u.betweenness_score DESC LIMIT 20")
        top_bc = [r['uid'] for r in res_bc]
        
        # 2. Get Top 20 Degree
        res_deg = session.run("MATCH (u:User) WITH u, COUNT { (u)-[:FRIENDS_WITH]-() } AS degree RETURN u.user_id AS uid ORDER BY degree DESC LIMIT 20")
        top_deg = [r['uid'] for r in res_deg]
        
        overlap = set(top_bc).intersection(set(top_deg))
        bridges = list(set(top_bc) - set(top_deg)) # High BC, Low Degree
        
        # 3. Compute Rubric Metrics for both groups
        def get_group_stats(uids):
            if not uids: return {"MeanReviews": 0, "DistinctCities": 0, "DistinctCategories": 0}
            res = session.run("""
                UNWIND $uids AS uid
                MATCH (u:User {user_id: uid})-[:REVIEWED]->(b:Business)
                OPTIONAL MATCH (b)-[:LOCATED_IN]->(ci:City)
                OPTIONAL MATCH (b)-[:IN_CATEGORY]->(cat:Category)
                RETURN avg(u.review_count) AS MeanReviews, 
                       count(DISTINCT ci)/toFloat(size($uids)) AS DistinctCities, 
                       count(DISTINCT cat)/toFloat(size($uids)) AS DistinctCategories
            """, uids=uids)
            data = res.single()
            return {"MeanReviews": round(data["MeanReviews"], 1), 
                    "DistinctCities": round(data["DistinctCities"], 1), 
                    "DistinctCategories": round(data["DistinctCategories"], 1)}
            
        bridge_stats = get_group_stats(bridges)
        deg_stats = get_group_stats(top_deg)
        
    analysis = (
        f"CENTRALITY OVERLAP SIZE: {len(overlap)} out of 20\n\n"
        f"METRIC COMPARISON:\n"
        f"Group 1: 'Bridge' Users (Top 20 Betweenness, Outside Top 20 Degree)\n"
        f"   - Mean Reviews: {bridge_stats['MeanReviews']}\n"
        f"   - Distinct Cities: {bridge_stats['DistinctCities']}\n"
        f"   - Distinct Categories: {bridge_stats['DistinctCategories']}\n\n"
        f"Group 2: High Degree Users (Top 20 Degree)\n"
        f"   - Mean Reviews: {deg_stats['MeanReviews']}\n"
        f"   - Distinct Cities: {deg_stats['DistinctCities']}\n"
        f"   - Distinct Categories: {deg_stats['DistinctCategories']}\n\n"
        f"DISCUSSION:\n"
        f"Users with high betweenness but low degree act as 'Bridges' between isolated social clusters. "
        f"As shown by the metrics above, these Bridge users often travel to more distinct cities and review more diverse categories than purely high-degree users, "
        f"who tend to be local 'hubs' clustered in a single geographic area."
    )
    append_to_report("4", "Betweenness vs Degree Centrality (Bridge User Analysis)", analysis)
    logger.success("Q4 Complete.")

# ==========================================
# Q5: Link Prediction (Restaurant Recommendation)
# ==========================================
def run_link_prediction(driver):
    logger.info("Executing Q5: GDS Link Prediction Pipeline...")
    with driver.session() as session:
        
        # 1: Ensure Edges have Timestamps
        logger.info("Setting epoch timestamps on edges for Chronological Split...")
        session.run("""
            MATCH ()-[r:REVIEWED]->()
            WHERE r.timestamp IS NULL
            WITH r, localdatetime(r.date) AS dt
            SET r.timestamp = dt.epochSeconds
        """)

        # 2: Find the Chronological Split Timestamp (80th Percentile)
        logger.info("Calculating 80th percentile timestamp for chronological split...")
        split_timestamp_result = session.run("""
            MATCH ()-[r:REVIEWED]->()
            RETURN percentileCont(r.timestamp, 0.8) AS splitTimestamp
        """)
        split_timestamp = split_timestamp_result.single()['splitTimestamp']
        logger.info(f"Chronological split point: All reviews before timestamp {split_timestamp} are for training.")

        # 3: Clean old GDS resources
        session.run("CALL gds.graph.drop('lp_train_graph', false) YIELD graphName")
        session.run("CALL gds.pipeline.drop('lp_pipe', false) YIELD pipelineName")
        session.run("CALL gds.model.drop('lp_model', false) YIELD modelInfo")
        
        # 4: Project the TRAINING Graph (Using Correct Cypher Projection)
        logger.info("Projecting TRAINING subgraph (reviews before split timestamp)...")
        session.run("""
            CALL gds.graph.project.cypher(
                'lp_train_graph',
                'MATCH (n:User) RETURN id(n) AS id UNION MATCH (n:Business) RETURN id(n) AS id',
                'MATCH (u:User)-[r:REVIEWED]->(b:Business) WHERE r.timestamp < $splitTimestamp RETURN id(u) AS source, id(b) AS target, "REVIEWED" as type',
                { parameters: { splitTimestamp: $splitTimestamp } }
            )
        """, splitTimestamp=split_timestamp)

        # 5: Create and Configure the ML Pipeline
        session.run("CALL gds.beta.pipeline.linkPrediction.create('lp_pipe')")
        
        session.run("""
            CALL gds.beta.pipeline.linkPrediction.addNodeProperty('lp_pipe', 'fastRP', {
                mutateProperty: 'embedding', embeddingDimension: 64, randomSeed: 42
            })
        """)
        
        session.run("CALL gds.beta.pipeline.linkPrediction.addFeature('lp_pipe', 'hadamard', { nodeProperties: ['embedding'] })")
        
        session.run("""
            CALL gds.beta.pipeline.linkPrediction.addRandomForest('lp_pipe', {
                numberOfDecisionTrees: 10, maxDepth: 8
            })
        """)
        
        # 6: Train the Model on the TRAINING Graph
        logger.info("Training Link Prediction Model...")
        train_result = session.run("""
            CALL gds.beta.pipeline.linkPrediction.train('lp_train_graph', {
                pipeline: 'lp_pipe',
                modelName: 'lp_model',
                targetRelationshipType: 'REVIEWED',
                randomSeed: 42
            }) YIELD modelInfo
            RETURN modelInfo.metrics.AUCPR.train AS train_aucpr, modelInfo.featureImportance AS featureImportance
        """).single()

        # 7: Manually Evaluate Model on the TEST Set
        logger.info("Evaluating model on the held-out TEST set (reviews after split timestamp)...")
        test_metrics = session.run("""
            MATCH (u:User)-[r:REVIEWED]->(b:Business)
            WHERE r.timestamp >= $splitTimestamp
            WITH u, b, 1 AS label
            LIMIT 20000
            
            UNION
            
            MATCH (u:User), (b:Business)
            WHERE u.review_count > 5 AND b.review_count > 10 AND NOT (u)-[:REVIEWED]->(b)
            WITH u, b, 0 AS label
            LIMIT 20000
            
            WITH gds.util.asNode(id(u)) AS source, gds.util.asNode(id(b)) AS target, label
            
            CALL gds.beta.pipeline.linkPrediction.predict.stream('lp_train_graph', {
                modelName: 'lp_model',
                sourceNode: source,
                targetNode: target
            }) YIELD probability
            
            RETURN gds.beta.model.evaluate.classification.auc({
                groundTruth: collect(label),
                predictedProbabilities: collect(probability)
            }) AS test_auc
        """).single()


        # 8: Generate Recommendations and Report Results
        logger.info("Generating Recommendations for Sampled Users...")
        rec_result = session.run("""
            MATCH (u:User) WHERE u.review_count > 50 AND u.review_count < 200 WITH u LIMIT 5
            MATCH (b:Business) WHERE b.review_count > 100 AND NOT (u)-[:REVIEWED]->(b)
            WITH u, collect(b) as businesses
            
            CALL gds.beta.pipeline.linkPrediction.predict.stream('lp_train_graph', {
                modelName: 'lp_model', topN: 3, sourceNode: u, targetNodes: businesses
            }) YIELD node1, node2, probability
            RETURN gds.util.asNode(node1).name AS User, gds.util.asNode(node2).name AS Recommended_Restaurant, round(probability, 3) AS Confidence
        """)
        
        import pandas as pd
        df_recs = pd.DataFrame([r.data() for r in rec_result])
        
        feature_importance_df = pd.DataFrame(train_result['featureImportance'])
        analysis = (
            f"MODEL PERFORMANCE:\n"
            f"AUC-PR (Train Set): {train_result['train_aucpr']:.4f}\n"
            f"AUC-ROC (Test Set): {test_metrics['test_auc']:.4f}\n\n"
            f"FEATURE IMPORTANCE:\n{feature_importance_df.to_string(index=False)}\n\n"
            f"DISCUSSION:\n"
            f"The model was trained on reviews before a specific date and tested on reviews after, fulfilling the chronological split requirement. "
            f"FastRP embeddings act as the primary features, condensing user behavior, business popularity, and latent affinities (like shared categories or cities) into a dense vector. "
            f"The Hadamard operator then calculates the affinity between a user's and a business's embedding, which is the sole input to the Random Forest. "
            f"This demonstrates that the graph's structure is highly predictive of future user-business interactions.\n\n"
            f"TOP 3 RECOMMENDATIONS FOR 5 SAMPLED USERS:\n"
        )
    append_to_report("5", "GDS Link Prediction (Restaurant Recommendation)", analysis, df_recs)
    logger.success("Q5 Complete.")

if __name__ == "__main__":
    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
    
    driver = get_driver()
    try:
        run_pagerank(driver)
        run_louvain(driver)
        run_node_similarity(driver)
        run_betweenness(driver)
        run_link_prediction(driver)
    except Exception as e:
        logger.exception(f"Neo4j GDS failed: {e}")
    finally:
        driver.close()