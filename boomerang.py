import json
import geoip2.database
from collections import defaultdict
from tqdm import tqdm
import psutil

# File paths (adjust as needed)
TRACEROUTE_PATH = r'E:\internet-graph-master\dataset\traceroute-2024-10-01T0000'
GEOIP2_PATH = r'E:\internet-graph-master\dataset\GeoIP2-City.mmdb'

# Constants
MAX_RAM_USAGE_RATIO = 0.75  # Use max 75% of available RAM

# Data Privacy Laws Mapping
LEGAL_FRAMEWORKS = {
    "GDPR": ["AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IS", "IE", "IT", 
             "LV", "LI", "LT", "LU", "MT", "NL", "NO", "PL", "PT", "RO", "SK", "SI", "ES", "SE", "CH"],
    "CCPA": ["US"],  # USA, focused primarily on California
    "PIPEDA": ["CA"],  # Canada
    "LGPD": ["BR"],  # Brazil
    "APPI": ["JP"],  # Japan
    "PDPA": ["SG", "TH", "MY"],  # Singapore, Thailand, Malaysia
    "Other": []  # Placeholder for countries not covered by specific laws
}

# Function to check current memory usage and halt processing if memory exceeds limit
def check_memory():
    mem = psutil.virtual_memory()
    return mem.percent / 100 < MAX_RAM_USAGE_RATIO

# GeoIP reader
geoip_reader = geoip2.database.Reader(GEOIP2_PATH)

# Initialize statistics storage per legal framework
framework_stats = defaultdict(lambda: defaultdict(int))

# 1. Helper function to get country and its legal framework from IP address
def get_country_and_framework(ip_address):
    try:
        response = geoip_reader.city(ip_address)
        country = response.country.iso_code
        for framework, countries in LEGAL_FRAMEWORKS.items():
            if country in countries:
                return framework, country
        return "Other", country  # If no framework matches, label as "Other"
    except Exception:
        return None, None

# 2. Traceroute Data Parsing with variable structure handling and progress display
def parse_traceroute_data(file_path):
    # Get total lines with latin-1 encoding to handle mixed characters
    total_lines = sum(1 for _ in open(file_path, 'r', encoding='latin-1'))

    with open(file_path, 'r', encoding='latin-1') as file, tqdm(total=total_lines, desc="Processing traceroute data") as pbar:
        for line in file:
            if not check_memory():
                break  # Stop if RAM usage exceeds limit

            try:
                record = json.loads(line)

                # Check if the necessary fields are present before processing
                if 'src_addr' not in record or 'dst_addr' not in record or 'af' not in record:
                    pbar.update(1)
                    continue  # Skip this line if critical data is missing

                src_ip = record['src_addr']
                dst_ip = record['dst_addr']
                af = 'IPv4' if record['af'] == 4 else 'IPv6'

                # Determine the legal framework and country for source and destination IPs
                src_framework, src_country = get_country_and_framework(src_ip)
                dst_framework, dst_country = get_country_and_framework(dst_ip)

                # If either source or destination is not associated with a legal framework, skip this line
                if src_framework is None or dst_framework is None:
                    pbar.update(1)
                    continue
                
                # Update IP counts by legal framework and type
                framework_stats[src_framework][f"Unique_{af}_as_source"] += 1
                framework_stats[dst_framework][f"Unique_{af}_as_destination"] += 1

                # Analyze hops if they are available
                foreign_hop = False
                if 'result' in record:
                    for hop in record['result']:
                        if 'result' not in hop:
                            continue  # Skip this hop if no 'result' field is present
                        
                        for result in hop['result']:
                            hop_ip = result.get('from')
                            if hop_ip:
                                hop_framework, hop_country = get_country_and_framework(hop_ip)
                                if hop_framework != src_framework:
                                    foreign_hop = True  # Foreign hop detected
                                framework_stats[src_framework][f"Unique_{af}_in_hops"] += 1

                # Path classification for boomerang and non-boomerang paths
                framework_stats[src_framework]["Total_paths"] += 1
                if src_framework == dst_framework:
                    framework_stats[src_framework]["Intra-framework_paths"] += 1
                    if foreign_hop:
                        framework_stats[src_framework]["Boomerang_paths"] += 1
                    else:
                        framework_stats[src_framework]["No_foreign_hop_paths"] += 1

            except (json.JSONDecodeError, TypeError):
                continue  # Skip lines that are invalid JSON or have incompatible types

            pbar.update(1)  # Update progress bar


# Execute function to parse traceroute data
parse_traceroute_data(TRACEROUTE_PATH)

# Close GeoIP reader
geoip_reader.close()

# 3. Format and display results
def display_statistics(framework_stats):
    for framework, stats in framework_stats.items():
        print(f"Legal Framework: {framework}")
        print(f"  Unique IPv4 as source: {stats['Unique_IPv4_as_source']}")
        print(f"  Unique IPv6 as source: {stats['Unique_IPv6_as_source']}")
        print(f"  Total unique IP addresses as source: {stats['Unique_IPv4_as_source'] + stats['Unique_IPv6_as_source']}")
        print(f"  Unique IPv4 in hops: {stats['Unique_IPv4_in_hops']}")
        print(f"  Unique IPv6 in hops: {stats['Unique_IPv6_in_hops']}")
        print(f"  Unique IPv4 as destination: {stats['Unique_IPv4_as_destination']}")
        print(f"  Unique IPv6 as destination: {stats['Unique_IPv6_as_destination']}")
        print(f"  Unique paths with same source and destination framework: {stats['Intra-framework_paths']}")
        print(f"  Unique paths with same source and destination framework but at least one foreign hop: {stats['Boomerang_paths']}")
        print(f"  Unique paths with same source and destination framework and no foreign hops: {stats['No_foreign_hop_paths']}")

# Run display
display_statistics(framework_stats)
