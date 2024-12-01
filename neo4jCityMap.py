from neo4j import GraphDatabase
import csv

# Random password for Neo4j
password = "12345678"

# Neo4j connection URI and credentials
uri = "bolt://localhost:7687"
username = "neo4j"

# Define the file path
file_path = r"E:\cityMap.csv"

# Initialize Neo4j driver
driver = GraphDatabase.driver(uri, auth=(username, password))

def create_city_map(tx, city1, city2, min_latency, median_latency, percentile_latency):
    # Create nodes and relationships in the database
    query = """
    MERGE (c1:City {id: $city1})
    MERGE (c2:City {id: $city2})
    MERGE (c1)-[r:CONNECTED_TO {
        min_latency: $min_latency, 
        median_latency: $median_latency, 
        percentile_latency: $percentile_latency
    }]->(c2)
    """
    tx.run(query, city1=city1, city2=city2, 
           min_latency=min_latency, 
           median_latency=median_latency, 
           percentile_latency=percentile_latency)

def process_csv(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        with driver.session() as session:
            for row in reader:
                city1 = row["City1_ID"]
                city2 = row["City2_ID"]
                min_latency = float(row["Min_Latency"])
                median_latency = float(row["Median_Latency"])
                percentile_latency = float(row["95th_Percentile_Latency"])
                session.write_transaction(
                    create_city_map, city1, city2, min_latency, median_latency, percentile_latency
                )

def main():
    print("Processing city map data...")
    process_csv(file_path)
    print("City map construction complete.")

if __name__ == "__main__":
    main()
