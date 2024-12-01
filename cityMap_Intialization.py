import json
import statistics
import threading
from neo4j import GraphDatabase
from collections import defaultdict
from geopy.distance import geodesic

# Database configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "213475656"

# Initialize Neo4j driver
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# In-memory data structure to store latencies
city_latencies = defaultdict(list)
lock = threading.Lock()

def create_city_node(tx, data):
    query = """
    MERGE (c:City {city_id: $city_id})
    SET c.geoname_id = $geoname_id,
        c.latitude = $latitude,
        c.longitude = $longitude,
        c.country_iso_code = $country_iso_code,
        c.median_latency = $median_latency,
        c.average_latency = $average_latency,
        c.min_latency = $min_latency,
        c.max_latency = $max_latency,
        c.mode_latency = $mode_latency,
        c.path_count = $path_count,
        c.geoip_source = $geoip_source,
        c.geo_accuracy_radius = $geo_accuracy_radius,
        c.ASN = $ASN,
        c.as_relationship = $as_relationship,
        c.distance_km = $distance_km
    """
    tx.run(query, **data)

def insert_data_into_neo4j(city_map_data):
    with driver.session(database="CityMap") as session:
        for data in city_map_data:
            session.write_transaction(create_city_node, data)

def process_traceroute_batch(batch, city_map_data):
    for line in batch:
        traceroute = json.loads(line)
        hops = traceroute.get("result", [])
        
        # Extracting latencies between consecutive hops
        for i in range(len(hops) - 1):
            from_ip = hops[i]["result"][0].get("from")
            to_ip = hops[i + 1]["result"][0].get("from")
            if not from_ip or not to_ip:
                continue

            # Use GeoIP to retrieve city information (pseudo code, replace with actual lookup)
            from_city = lookup_city(from_ip)
            to_city = lookup_city(to_ip)

            # Create city_id pair for tracking latencies
            if from_city and to_city:
                city_pair = f"{from_city['city_id']}#{to_city['city_id']}"
                latency = hops[i + 1]["result"][0].get("rtt", 0)
                
                if latency:
                    with lock:
                        city_latencies[city_pair].append(latency)

        # After processing each batch, calculate statistics and prepare data for Neo4j
        for city_pair, latencies in city_latencies.items():
            city1, city2 = city_pair.split("#")
            
            median_latency = statistics.median(latencies)
            average_latency = statistics.mean(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            mode_latency = statistics.mode(latencies) if len(latencies) > 1 else latencies[0]
            path_count = len(latencies)
            
            # Prepare data dictionary for Neo4j
            city_data = {
                "city_id": city1,
                "geoname_id": from_city.get("geoname_id"),
                "latitude": from_city.get("latitude"),
                "longitude": from_city.get("longitude"),
                "country_iso_code": from_city.get("country_iso_code"),
                "median_latency": median_latency,
                "average_latency": average_latency,
                "min_latency": min_latency,
                "max_latency": max_latency,
                "mode_latency": mode_latency,
                "path_count": path_count,
                "geoip_source": from_city.get("geoip_source"),
                "geo_accuracy_radius": from_city.get("geo_accuracy_radius"),
                "ASN": from_city.get("ASN"),
                "as_relationship": "peer-to-peer",  # replace with actual relationship from dataset
                "distance_km": geodesic((from_city.get("latitude"), from_city.get("longitude")),
                                        (to_city.get("latitude"), to_city.get("longitude"))).km
            }
            
            city_map_data.append(city_data)

def batch_process_traceroute_file(filepath, batch_size=1000, max_lines=10000):
    with open(filepath, "r") as f:
        lines = []
        city_map_data = []
        
        for line_num, line in enumerate(f):
            if line_num >= max_lines:
                break
            
            lines.append(line)
            
            if len(lines) >= batch_size:
                # Multithreading
                thread = threading.Thread(target=process_traceroute_batch, args=(lines.copy(), city_map_data))
                thread.start()
                thread.join()
                lines.clear()

        # Process any remaining lines
        if lines:
            process_traceroute_batch(lines, city_map_data)
        
        # Insert data into Neo4j
        insert_data_into_neo4j(city_map_data)

def lookup_city(ip_address):
    # Placeholder for GeoIP lookup (e.g., using MaxMind GeoIP2)
    # Return a dictionary with city_id, geoname_id, latitude, longitude, etc.
    return {
        "city_id": "SampleCity#SampleSubdiv#SampleCountry",
        "geoname_id": 12345,
        "latitude": 12.34,
        "longitude": 56.78,
        "country_iso_code": "US",
        "geoip_source": "GeoIP2",
        "geo_accuracy_radius": 50,
        "ASN": 1234
    }

# Run the processing function
batch_process_traceroute_file(r'E:\internet-graph-master\dataset\traceroute-2024-10-01T0000')
