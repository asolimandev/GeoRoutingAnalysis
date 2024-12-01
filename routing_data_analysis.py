import json
import geoip2.database
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
import os

# File paths
TRACEROUTE_PATH = r'E:\internet-graph-master\dataset\traceroute-2024-10-01T0000'
AS_REL_PATH = r'E:\internet-graph-master\dataset\20241001.as-rel2.txt'
GEOIP2_PATH = r'E:\internet-graph-master\dataset\GeoIP2-City.mmdb'
GEOLITE2_PATH = r'E:\internet-graph-master\dataset\GeoLite2-City.mmdb'

# Constants
MAX_RAM_USAGE_RATIO = 0.75  # Use max 75% of available RAM

# Function to check current memory usage and halt processing if memory exceeds limit
def check_memory():
    mem = psutil.virtual_memory()
    return mem.percent / 100 < MAX_RAM_USAGE_RATIO

# 1. IP Address and Path Statistics
def parse_traceroute_data(file_path):
    stats = defaultdict(lambda: defaultdict(int))
    path_lengths = []
    
    with open(file_path, 'r') as file:
        for line in file:
            if not check_memory():
                break  # Stop if RAM usage exceeds limit

            try:
                record = json.loads(line)
                country = get_country_from_ip(record['src_addr'])
                ip_version = 'IPv4' if record['af'] == 4 else 'IPv6'
                
                # Count IP addresses and path stats
                stats[country][f"Unique_{ip_version}_src"] += 1
                path_lengths.append(len(record.get('result', [])))

            except json.JSONDecodeError:
                continue  # Skip invalid lines

    return stats, Counter(path_lengths)

def get_country_from_ip(ip_address):
    # Placeholder function, assuming geoip database is loaded and queried here
    return "US"  # Example placeholder

# 2. Traceroute Dataset Path Analysis
def analyze_path_lengths(traceroute_stats):
    path_summary = {
        "min_length": min(traceroute_stats.values()),
        "max_length": max(traceroute_stats.values()),
        "avg_length": sum(traceroute_stats.values()) / len(traceroute_stats),
        "median_length": sorted(traceroute_stats.values())[len(traceroute_stats) // 2]
    }
    return path_summary

# 3. AS Relationship Analysis
def parse_as_relationships(file_path):
    as_stats = defaultdict(lambda: {"peers": 0, "providers": 0, "customers": 0})

    with open(file_path, 'r') as file:
        for line in file:
            if not line.startswith('#') and check_memory():
                try:
                    src_as, dest_as, rel_type = map(int, line.strip().split('|'))
                    if rel_type == 0:
                        as_stats[src_as]["peers"] += 1
                    elif rel_type == -1:
                        as_stats[src_as]["providers"] += 1
                        as_stats[dest_as]["customers"] += 1
                    else:
                        as_stats[src_as]["customers"] += 1
                except ValueError:
                    continue  # Skip malformed lines

    return as_stats

# 4. GeoIP Dataset Comparison
def compare_geoip_datasets(geoip_path1, geoip_path2):
    with geoip2.database.Reader(geoip_path1) as reader1, geoip2.database.Reader(geoip_path2) as reader2:
        ip_comparisons = {
            "total_entries1": 0,
            "total_entries2": 0,
            "matching_entries": 0
        }
        # Iterate over IPs in both datasets
        # Placeholder for actual IP data loading and comparison logic here
    return ip_comparisons

# Multithreading management function
def process_datasets():
    traceroute_stats = {}
    as_stats = {}
    geoip_comparison = {}

    with ThreadPoolExecutor() as executor:
        # Define tasks
        tasks = {
            "traceroute": executor.submit(parse_traceroute_data, TRACEROUTE_PATH),
            "as_relationships": executor.submit(parse_as_relationships, AS_REL_PATH),
            "geoip_comparison": executor.submit(compare_geoip_datasets, GEOIP2_PATH, GEOLITE2_PATH)
        }

        for task_name, future in as_completed(tasks.values()):
            if task_name == "traceroute":
                traceroute_stats = future.result()
            elif task_name == "as_relationships":
                as_stats = future.result()
            elif task_name == "geoip_comparison":
                geoip_comparison = future.result()
            
    return traceroute_stats, as_stats, geoip_comparison

# Run processing and output summary
if __name__ == "__main__":
    traceroute_data, as_data, geoip_data = process_datasets()
    
    # Display summary statistics (Example output)
    print("Traceroute Data Summary:", analyze_path_lengths(traceroute_data))
    print("AS Relationships Summary:", as_data)
    print("GeoIP Dataset Comparison:", geoip_data)
