import os
import json
import numpy as np
from neo4j import GraphDatabase
import geoip2.database

# Configurations
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"
GEOIP_DB_PATH = r"E:/internet-graph-master/dataset/GeoIP2-City.mmdb"
TRACEROUTE_FILE = r"E:/internet-graph-master/dataset/traceroute-2024-10-01T0000"
OUTPUT_STATS_FILE = r"E:/internet-graph-master/output/traceroute_final_stats.txt"

# Variable jurisdictions
SOURCE_JURISDICTION = "BR"  # Source jurisdiction to analyze
AVOID_JURISDICTION = "CL"   # Jurisdiction to avoid in paths

# GDPR countries treated as one jurisdiction
GDPR_COUNTRIES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "EL", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE"
}

# Weight selection: Choose from "min_latency", "median_latency", or "percentile_latency"
SELECTED_WEIGHT = "min_latency"

def connect_to_neo4j():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def city_id_from_ip(ip, geoip_reader):
    """Get the city ID from an IP address."""
    try:
        response = geoip_reader.city(ip)
        city = response.city.name
        subdivision = response.subdivisions.most_specific.name
        country = response.country.iso_code

        # Skip entries with incomplete or unknown data
        if not city or not subdivision or not country or \
           city == "Unknown" or subdivision == "Unknown" or country == "Unknown":
            return None, None

        return f"{city}#{subdivision}#{country}", country
    except geoip2.errors.AddressNotFoundError:
        return None, None

def get_jurisdictions(countries):
    """Group GDPR countries as one jurisdiction."""
    jurisdictions = set()
    for country in countries:
        if country in GDPR_COUNTRIES:
            jurisdictions.add("GDPR")
        else:
            jurisdictions.add(country)
    return jurisdictions

def query_city_map(tx, city1, city2, weight_property):
    """Query the city map for a specific latency weight."""
    query = f"""
    MATCH (c1:City {{id: $city1}})-[r:CONNECTED_TO]->(c2:City {{id: $city2}})
    RETURN r.{weight_property} AS latency
    """
    result = tx.run(query, city1=city1, city2=city2)
    return [record["latency"] for record in result]

def find_shortest_path(tx, source_cityid, target_cityid, weight):
    """
    Finds the shortest path between two cities using Neo4j's default shortest path function.
    """
    try:
        query = """
        MATCH (start:City {id: $source_cityid}), (end:City {id: $target_cityid})
        MATCH path = shortestPath((start)-[*]->(end))
        RETURN length(path) AS city_hop_count, 
               size(apoc.coll.toSet([CASE WHEN n.country IN $gdpr THEN 'GDPR' ELSE n.country END FOR n IN nodes(path)])) AS jurisdiction_hop_count,
               reduce(latency = 0, r in relationships(path) | latency + coalesce(r[$weight], 0)) AS total_latency
        """
        result = tx.run(query, source_cityid=source_cityid, target_cityid=target_cityid, weight=weight, gdpr=list(GDPR_COUNTRIES)).single()

        if result:
            return {
                "latency": result["total_latency"],
                "city_hop_count": result["city_hop_count"],
                "jurisdiction_hop_count": result["jurisdiction_hop_count"],
            }
        else:
            return None

    except Exception as e:
        print(f"Error in find_shortest_path: {e}")
        return None

def calculate_statistics(data):
    """Calculate statistics for latency data."""
    if not data:
        return {}
    return {
        "min": np.min(data),
        "max": np.max(data),
        "median": np.median(data),
        "mean": np.mean(data),
        "25th_percentile": np.percentile(data, 25),
        "75th_percentile": np.percentile(data, 75),
        "95th_percentile": np.percentile(data, 95),
    }

def write_stats(output, label, stats):
    """Write statistics to output file."""
    output.write(f"\nStatistics for {label}:\n")
    for stat_name, value in stats.items():
        output.write(f"{stat_name}: {value:.2f}\n")

def process_traceroute_file():
    with open(TRACEROUTE_FILE, "r") as file, open(OUTPUT_STATS_FILE, "w") as output, geoip2.database.Reader(GEOIP_DB_PATH) as geoip:
        driver = connect_to_neo4j()
        source_to_avoid_paths = 0
        alternative_paths_found = 0

        original_latencies, original_city_hops, original_jurisdiction_hops = [], [], []
        alternative_latencies, alternative_city_hops, alternative_jurisdiction_hops = [], [], []

        for line in file:
            data = json.loads(line)
            src_ip = data["src_addr"]
            hops = [hop["result"][0]["from"] for hop in data.get("result", []) if "result" in hop]

            src_city, src_country = city_id_from_ip(src_ip, geoip)
            if not src_city or src_country != SOURCE_JURISDICTION:
                continue

            hop_countries = []
            hop_cities = []

            for hop in hops:
                city, country = city_id_from_ip(hop, geoip)
                if city and country:
                    hop_cities.append(city)
                    hop_countries.append(country)

            jurisdictions = get_jurisdictions(hop_countries)

            if AVOID_JURISDICTION in hop_countries and hop_countries[-1] != AVOID_JURISDICTION:
                source_to_avoid_paths += 1
                with driver.session() as session:
                    for i in range(len(hop_cities) - 1):
                        city1 = hop_cities[i]
                        city2 = hop_cities[i + 1]
                        try:
                            # Original path latencies
                            latencies_query = session.read_transaction(query_city_map, city1, city2, SELECTED_WEIGHT)
                            if latencies_query:
                                latency = latencies_query[0]
                                original_latencies.append(latency)
                                original_city_hops.append(len(hop_cities))
                                original_jurisdiction_hops.append(len(jurisdictions))

                            # Alternative path avoiding the jurisdiction to avoid
                            if AVOID_JURISDICTION in hop_countries:
                                alt_path = session.read_transaction(find_shortest_path, city1, city2, SELECTED_WEIGHT)
                                if alt_path:
                                    alternative_latencies.append(alt_path["latency"])
                                    alternative_city_hops.append(alt_path["city_hop_count"])
                                    alternative_jurisdiction_hops.append(alt_path["jurisdiction_hop_count"])
                                    alternative_paths_found += 1
                        except Exception:
                            continue

        # Calculate and write statistics
        write_stats(output, f"Original Path Latency ({SELECTED_WEIGHT})", calculate_statistics(original_latencies))
        write_stats(output, "Original City Hop Count", calculate_statistics(original_city_hops))
        write_stats(output, "Original Jurisdiction Hop Count", calculate_statistics(original_jurisdiction_hops))

        write_stats(output, f"Alternative Path Latency ({SELECTED_WEIGHT})", calculate_statistics(alternative_latencies))
        write_stats(output, "Alternative City Hop Count", calculate_statistics(alternative_city_hops))
        write_stats(output, "Alternative Jurisdiction Hop Count", calculate_statistics(alternative_jurisdiction_hops))

        output.write(f"\nTotal {SOURCE_JURISDICTION}->Paths Avoiding {AVOID_JURISDICTION}: {source_to_avoid_paths}\n")
        output.write(f"Alternative paths avoiding {AVOID_JURISDICTION}: {alternative_paths_found}\n")

if __name__ == "__main__":
    process_traceroute_file()
