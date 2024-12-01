# --- Imports ---
import geoip2.database
import logging
import json
from collections import defaultdict
from tqdm import tqdm
import ipaddress
import os
from statistics import median, mode, StatisticsError
import statistics
from typing import Optional, Tuple

# --- File Paths ---
statistics_file = "latency_statistics2.txt"
latency_json_file = "latency_data2.json"
geoip_db_path = 'E:/internet-graph-master/dataset/GeoIP2-City.mmdb'
traceroute_file_path = "E:/internet-graph-master/dataset/traceroute-2024-10-01T0000"
bogon_ipv4_path = 'E:/internet-graph-master/dataset/fullbogons-ipv4.txt'
bogon_ipv6_path = 'E:/internet-graph-master/dataset/fullbogons-ipv6.txt'

# --- GDPR Countries ---
gdpr_countries = {
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", 
    "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"
}

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

# --- GeoIP Reader ---
geoip_reader = geoip2.database.Reader(geoip_db_path)

# --- Statistics and Counters ---
total_ip_addresses = 0
unique_ip_addresses = set()
total_latencies = 0
unique_cities = set()
unique_countries = set()
unique_subdivisions = set()
missing_city_name_counter = 0
missing_country_counter = 0
private_or_cgnat_ip_counter = 0

# IPv4 and IPv6 Count Trackers
ipv4_count = 0
ipv6_count = 0
unique_ipv4 = set()
unique_ipv6 = set()

# Source, Hop, and Destination IP Count Trackers
source_ipv4_count = 0
source_ipv6_count = 0
hop_ipv4_count = 0
hop_ipv6_count = 0
destination_ipv4_count = 0
destination_ipv6_count = 0

# --- Country-Specific Statistics ---
country_stats = defaultdict(lambda: {
    "total": {"ipv4": 0, "ipv6": 0, "unique_ipv4": set(), "unique_ipv6": set()},
    "source": {"ipv4": 0, "ipv6": 0, "unique_ipv4": set(), "unique_ipv6": set()},
    "hop": {"ipv4": 0, "ipv6": 0, "unique_ipv4": set(), "unique_ipv6": set()},
    "destination": {"ipv4": 0, "ipv6": 0, "unique_ipv4": set(), "unique_ipv6": set()},
    "path_counts": {
        "source_only": 0,
        "source_destination": 0,
        "source_all_hops_destination": 0,
        "source_with_other_country": defaultdict(int),
        "boomerang_paths": defaultdict(lambda: {"total": 0, "per_traceroute": defaultdict(int)})
    }
})

# --- Latency Data ---
latency_data = defaultdict(lambda: defaultdict(lambda: {
    "latency_count": 0,
    "latencies": [],
    "country_code": "",
    "longitude": 0.0,
    "latitude": 0.0,
    "asn": 0,
    "accuracy_radius": 0,
    "min_latency": None,
    "max_latency": None,
    "average_latency": None,
    "median_latency": None,
    "mode_latency": None,
    "path_count": 0,
    "as_relationship": "",
    "distance_km": 0.0
}))

# --- Bogon IP Counters ---
total_bogon_ipv4 = 0
total_bogon_ipv6 = 0
unique_bogon_ipv4 = set()
unique_bogon_ipv6 = set()

# Bogon Count per Country
bogon_ipv4_per_country = defaultdict(int)
bogon_ipv6_per_country = defaultdict(int)
unique_bogon_ipv4_per_country = defaultdict(set)
unique_bogon_ipv6_per_country = defaultdict(set)

# Initialize metadata dictionaries for city pairs
country_code_dict = {}         # Dictionary mapping city_id to country code
longitude_dict = {}            # Dictionary mapping city_id to longitude
latitude_dict = {}             # Dictionary mapping city_id to latitude
asn_dict = {}                  # Dictionary mapping city_id to ASN
accuracy_radius_dict = {}      # Dictionary mapping city_id to accuracy radius
distance_dict = {}             # Dictionary mapping city_id pairs to distance in km
as_relationship_dict = {}      # Dictionary mapping city_id pairs to AS relationship

def is_private_or_cgnat_ip(ip: str) -> bool:
    """
    Check if the given IP address is within private or CGNAT (Carrier-Grade NAT) ranges.

    Args:
        ip (str): The IP address to check.

    Returns:
        bool: True if the IP is private or within the CGNAT range; otherwise, False.
    """
    try:
        ip_obj = ipaddress.ip_address(ip)
        # Check if the IP is private (e.g., 192.168.0.0/16, 10.0.0.0/8, etc.)
        if ip_obj.is_private:
            return True
        # Check if the IP is in the CGNAT range (100.64.0.0/10)
        if ip_obj in ipaddress.ip_network("100.64.0.0/10"):
            return True
    except ValueError:
        # Handle invalid IP address input
        logging.error(f"Invalid IP address encountered: {ip}")
        return False
    
    return False


def load_bogon_ips(file_path: str) -> set:
    """
    Load bogon IPs from a specified file and return them as a set of IP network objects.
    
    Args:
        file_path (str): The path to the bogon IPs file.

    Returns:
        set: A set containing `ipaddress.IPv4Network` and `ipaddress.IPv6Network` objects for each bogon range.
    """
    bogon_ips = set()

    try:
        with open(file_path, "r") as file:
            for line in file:
                # Skip comments and blank lines
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                
                # Attempt to parse the line as an IP network
                try:
                    network = ipaddress.ip_network(line)
                    bogon_ips.add(network)
                except ValueError:
                    # Log if the line is not a valid network
                    logging.error(f"Invalid bogon IP range found: {line}")
    
    except FileNotFoundError:
        logging.error(f"Bogon IP file not found at path: {file_path}")
    except IOError as e:
        logging.error(f"Error reading bogon IP file at {file_path}: {e}")

    return bogon_ips


def is_bogon_ip(ip: str, bogon_set: set) -> bool:
    """
    Check if a given IP address falls within any of the bogon IP ranges.

    Args:
        ip (str): The IP address to check.
        bogon_set (set): A set containing `ipaddress.IPv4Network` and `ipaddress.IPv6Network` objects representing bogon ranges.

    Returns:
        bool: True if the IP address is within a bogon range; False otherwise.
    """
    try:
        ip_obj = ipaddress.ip_address(ip)
        
        # Check if the IP is within any of the bogon ranges in the set
        for bogon_range in bogon_set:
            if ip_obj in bogon_range:
                return True  # IP is a bogon

        return False  # IP is not a bogon

    except ValueError:
        logging.error(f"Invalid IP address format: {ip}")
        return False

def get_country_label(country_code: str) -> str:
    """
    Returns 'GDPR' if the given country code is part of the GDPR list; otherwise, returns the country code itself.

    Args:
        country_code (str): The ISO country code to check.

    Returns:
        str: 'GDPR' if the country is GDPR-covered, otherwise the original country code.
    """
    # Define GDPR-covered countries as a set for efficient lookup
    gdpr_countries = {
        "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", 
        "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"
    }
    
    # Return 'GDPR' if the country is in the GDPR list, else return the original code
    return "GDPR" if country_code in gdpr_countries else country_code

def city_id_from_ip(ip: str, bogon_ipv4_set: set, bogon_ipv6_set: set) -> Tuple[Optional[str], Optional[str]]:
    """
    Retrieves the city identifier and country label for a given IP address using GeoIP database.
    If not found in GeoIP, checks if the IP is in the bogon dataset. If still not found, 
    checks if the IP is private or CGNAT.

    Args:
        ip (str): The IP address to look up.
        bogon_ipv4_set (set): Set of IPv4 bogon networks.
        bogon_ipv6_set (set): Set of IPv6 bogon networks.

    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing the city ID in the format
        "City#Subdivision#CountryLabel" and the country label. Returns (None, None) if the
        IP is not found or is filtered as bogon, private, or CGNAT.
    """
    global missing_city_name_counter, missing_country_counter, private_or_cgnat_ip_counter, total_bogon_ipv4, total_bogon_ipv6

    # Attempt GeoIP lookup
    try:
        response = geoip_reader.city(ip)
        city = response.city.name
        subdivision = response.subdivisions.most_specific.name
        country_code = response.country.iso_code
        country_label = get_country_label(country_code)

        # Track missing fields
        if not city:
            missing_city_name_counter += 1
        if not country_code:
            missing_country_counter += 1

        # Return formatted city ID and country label if available
        if city and country_code:
            unique_cities.add(city)
            unique_countries.add(country_label)
            if subdivision:
                unique_subdivisions.add(subdivision)
            city_id = f"{city}#{subdivision}#{country_label}"
            return city_id, country_label
    except geoip2.errors.AddressNotFoundError:
        logging.warning(f"IP {ip} not found in GeoIP database.")

    # Check if IP is in bogon dataset
    ip_obj = ipaddress.ip_address(ip)
    if (ip_obj.version == 4 and any(ip_obj in net for net in bogon_ipv4_set)) or \
       (ip_obj.version == 6 and any(ip_obj in net for net in bogon_ipv6_set)):
        if ip_obj.version == 4:
            total_bogon_ipv4 += 1
            unique_bogon_ipv4.add(ip)
        else:
            total_bogon_ipv6 += 1
            unique_bogon_ipv6.add(ip)
        return None, None

    # Check if IP is private or CGNAT
    if is_private_or_cgnat_ip(ip):
        private_or_cgnat_ip_counter += 1
        return None, None

    return None, None
def get_geoip_data(ip: str) -> dict:
    """
    Retrieves detailed geographical and network data for a given IP address from the GeoIP database.

    Args:
        ip (str): The IP address to look up.

    Returns:
        dict: A dictionary containing the GeoIP data fields for the IP, including:
            - 'CityID' : City identifier in the format "City#Subdivision#CountryLabel"
            - 'CountryCode' : Country ISO code
            - 'Latitude' : Latitude of the location
            - 'Longitude' : Longitude of the location
            - 'ASN' : Autonomous System Number associated with the IP
            - 'AccuracyRadius' : Accuracy radius in kilometers
            - 'CountryLabel' : GDPR-compliant label or country code
        If the IP address is private, CGNAT, or not found, returns an empty dictionary.
    """
    global missing_city_name_counter, missing_country_counter, private_or_cgnat_ip_counter

    # Check if IP is private or CGNAT
    if is_private_or_cgnat_ip(ip):
        private_or_cgnat_ip_counter += 1
        return {}

    geoip_data = {}
    
    try:
        # Perform GeoIP lookup
        response = geoip_reader.city(ip)

        # Extract and format necessary location and network information
        city = response.city.name
        subdivision = response.subdivisions.most_specific.name
        country_code = response.country.iso_code
        latitude = response.location.latitude
        longitude = response.location.longitude
        accuracy_radius = response.location.accuracy_radius
        asn = response.traits.autonomous_system_number
        country_label = get_country_label(country_code)

        # Handle missing fields
        if not city:
            missing_city_name_counter += 1
        if not country_code:
            missing_country_counter += 1

        # Only populate dictionary if required fields are available
        if city and country_code:
            city_id = f"{city}#{subdivision}#{country_label}"
            geoip_data = {
                "CityID": city_id,
                "CountryCode": country_code,
                "Latitude": latitude,
                "Longitude": longitude,
                "ASN": asn,
                "AccuracyRadius": accuracy_radius,
                "CountryLabel": country_label,
            }
            # Update unique sets with location data
            unique_cities.add(city)
            unique_countries.add(country_label)
            if subdivision:
                unique_subdivisions.add(subdivision)

    except geoip2.errors.AddressNotFoundError:
        # Log warning if IP is not found in GeoIP database
        logging.warning(f"IP {ip} not found in GeoIP database.")
    
    # Return populated geoip_data dictionary or an empty dictionary if lookup fails
    return geoip_data

def update_ip_stats(ip: str, category: str, country: str = None) -> None:
    """
    Updates statistics for a given IP address, categorized by its role (source, hop, or destination)
    and classifies it as either IPv4 or IPv6. Updates total, unique, and country-specific IP counts.

    Args:
        ip (str): The IP address to process.
        category (str): The role category of the IP (e.g., 'source', 'hop', 'destination').
        country (str, optional): The country associated with the IP address.
    """
    global total_ip_addresses, ipv4_count, ipv6_count
    global source_ipv4_count, source_ipv6_count, hop_ipv4_count, hop_ipv6_count, destination_ipv4_count, destination_ipv6_count

    try:
        # Convert IP address to IP object for version check
        ip_obj = ipaddress.ip_address(ip)
        total_ip_addresses += 1  # Increment total IP count

        # Determine if IP is IPv4 or IPv6 and update relevant counters
        if ip_obj.version == 4:
            ipv4_count += 1
            unique_ipv4.add(ip)
            if country:
                country_stats[country]["total"]["ipv4"] += 1
                country_stats[country]["total"]["unique_ipv4"].add(ip)
            if category == "source":
                source_ipv4_count += 1
                if country:
                    country_stats[country]["source"]["ipv4"] += 1
                    country_stats[country]["source"]["unique_ipv4"].add(ip)
            elif category == "hop":
                hop_ipv4_count += 1
                if country:
                    country_stats[country]["hop"]["ipv4"] += 1
                    country_stats[country]["hop"]["unique_ipv4"].add(ip)
            elif category == "destination":
                destination_ipv4_count += 1
                if country:
                    country_stats[country]["destination"]["ipv4"] += 1
                    country_stats[country]["destination"]["unique_ipv4"].add(ip)

        elif ip_obj.version == 6:
            ipv6_count += 1
            unique_ipv6.add(ip)
            if country:
                country_stats[country]["total"]["ipv6"] += 1
                country_stats[country]["total"]["unique_ipv6"].add(ip)
            if category == "source":
                source_ipv6_count += 1
                if country:
                    country_stats[country]["source"]["ipv6"] += 1
                    country_stats[country]["source"]["unique_ipv6"].add(ip)
            elif category == "hop":
                hop_ipv6_count += 1
                if country:
                    country_stats[country]["hop"]["ipv6"] += 1
                    country_stats[country]["hop"]["unique_ipv6"].add(ip)
            elif category == "destination":
                destination_ipv6_count += 1
                if country:
                    country_stats[country]["destination"]["ipv6"] += 1
                    country_stats[country]["destination"]["unique_ipv6"].add(ip)

        # Track unique IPs overall
        unique_ip_addresses.add(ip)

    except ValueError:
        logging.error(f"Invalid IP address encountered: {ip}")

def calculate_latency(rtt: float) -> float:
    """
    Calculates the one-way latency from the round-trip time (RTT) value.

    Args:
        rtt (float): The round-trip time in milliseconds.

    Returns:
        float: The calculated one-way latency in milliseconds.
    """
    return rtt / 2.0

def calculate_latency_statistics(latencies: list) -> dict:
    """
    Calculates statistical measures for a list of latencies, including min, max, 
    average, median, mode, and count.

    Args:
        latencies (list): List of latency values in milliseconds.

    Returns:
        dict: Dictionary containing statistical measures of latencies, 
              with keys 'min_latency', 'max_latency', 'avg_latency', 
              'median_latency', 'mode_latency', and 'count'.
    """
    if not latencies:
        # Return default values if the list is empty
        return {
            "min_latency": None,
            "max_latency": None,
            "avg_latency": None,
            "median_latency": None,
            "mode_latency": None,
            "count": 0
        }
    
    latency_stats = {
        "min_latency": min(latencies),
        "max_latency": max(latencies),
        "avg_latency": sum(latencies) / len(latencies),
        "median_latency": statistics.median(latencies),
        "mode_latency": None,
        "count": len(latencies)
    }
    
    # Calculate mode if possible, otherwise handle exceptions for multimodal data
    try:
        latency_stats["mode_latency"] = statistics.mode(latencies)
    except statistics.StatisticsError:
        # No unique mode; leave as None or choose an alternative representation if needed
        latency_stats["mode_latency"] = None
    
    return latency_stats

def process_traceroute_line(line: str) -> None:
    """
    Processes a single line of a traceroute file, extracting IPs, latency information,
    and geographic information, then updating relevant statistics and latency data structures.

    Args:
        line (str): A single JSON-formatted line from the traceroute file.
    """
    try:
        data = json.loads(line)
        src_addr = data.get("src_addr")
        dst_addr = data.get("dst_addr")
        results = data.get("result", [])

        # Initialize source city and country
        src_city_id = None
        src_country = None

        # Process source IP
        if src_addr:
            src_city_data = city_id_from_ip(src_addr)
            if src_city_data:  # Only proceed if valid data is returned
                src_city_id, src_country = src_city_data
                update_ip_stats(src_addr, "source", src_country)

        # Track countries on the path
        hop_countries = set()
        path_countries = []

        # Processing hops in the traceroute
        for hop in results:
            hop_results = hop.get("result", [])
            for hop_data in hop_results:
                from_ip = hop_data.get("from")
                rtt = hop_data.get("rtt")

                if from_ip and rtt is not None:
                    # Process hop IP
                    hop_city_data = city_id_from_ip(from_ip)
                    if hop_city_data:  # Only proceed if valid data is returned
                        hop_city_id, hop_country = hop_city_data
                        update_ip_stats(from_ip, "hop", hop_country)
                        if hop_country != src_country:
                            hop_countries.add(hop_country)
                        path_countries.append(hop_country)

                    # Update latency between cities if unique and src_city_id is available
                    if src_city_id and hop_city_id and src_city_id != hop_city_id:
                        latency = calculate_latency(rtt)
                        latency_data[src_city_id][hop_city_id]["latencies"].append(latency)
                        latency_data[src_city_id][hop_city_id]["latency_count"] += 1

                        # Calculate detailed latency statistics
                        stats = calculate_latency_statistics(latency_data[src_city_id][hop_city_id]["latencies"])
                        latency_data[src_city_id][hop_city_id].update(stats)

                        # Move to the next city in the path
                        src_city_id = hop_city_id

        # Processing destination IP
        if dst_addr:
            dst_city_data = city_id_from_ip(dst_addr)
            if dst_city_data:  # Only proceed if valid data is returned
                dst_city_id, dst_country = dst_city_data
                update_ip_stats(dst_addr, "destination", dst_country)

                # Capture final latency between last hop and destination
                if src_city_id and dst_city_id and src_city_id != dst_city_id:
                    avg_rtt = sum(hop_data.get("rtt", 0) for hop_data in hop_results if hop_data.get("rtt") is not None) / max(len(hop_results), 1)
                    latency = calculate_latency(avg_rtt)
                    latency_data[src_city_id][dst_city_id]["latencies"].append(latency)
                    latency_data[src_city_id][dst_city_id]["latency_count"] += 1
                    stats = calculate_latency_statistics(latency_data[src_city_id][dst_city_id]["latencies"])
                    latency_data[src_city_id][dst_city_id].update(stats)

        # Handle boomerang paths
        if src_country and dst_country and src_country == dst_country and hop_countries:
            for hop_country in hop_countries:
                if hop_country and hop_country != src_country:
                    country_stats[src_country]["path_counts"]["boomerang_paths"][hop_country]["total"] += 1
                    country_stats[src_country]["path_counts"]["boomerang_paths"][hop_country]["per_traceroute"][line] += 1

    except json.JSONDecodeError:
        logging.error("Error decoding JSON for line.")
    except Exception as e:
        logging.error(f"Unexpected error processing line: {e}")


def write_statistics_to_file() -> None:
    """
    Writes gathered statistics to a designated text file, including IP counts, unique IP addresses,
    bogon counts, latency data, and country-specific path information.
    """

    # Ensure output files do not exist before writing
    if os.path.exists(statistics_file):
        os.remove(statistics_file)
    if os.path.exists(latency_json_file):
        os.remove(latency_json_file)

    # Writing general statistics to the text file
    with open(statistics_file, "w") as file:
        file.write(f"Total IP addresses processed: {total_ip_addresses}\n")
        file.write(f"Unique IP addresses: {len(unique_ip_addresses)}\n")
        file.write(f"Total latencies recorded: {total_latencies}\n")
        file.write(f"Unique cities: {len(unique_cities)}\n")
        file.write(f"Unique countries: {len(unique_countries)}\n")
        file.write(f"Unique subdivisions: {len(unique_subdivisions)}\n")
        file.write(f"Missing city names: {missing_city_name_counter}\n")
        file.write(f"Missing countries: {missing_country_counter}\n")
        file.write(f"Private or CGNAT IP addresses skipped: {private_or_cgnat_ip_counter}\n")
        file.write(f"Total IPv4 addresses: {ipv4_count}\n")
        file.write(f"Total IPv6 addresses: {ipv6_count}\n")
        file.write(f"Unique IPv4 addresses: {len(unique_ipv4)}\n")
        file.write(f"Unique IPv6 addresses: {len(unique_ipv6)}\n")
        file.write(f"Total bogon IPv4 addresses detected: {total_bogon_ipv4}\n")
        file.write(f"Total unique bogon IPv4 addresses: {len(unique_bogon_ipv4)}\n")
        file.write(f"Total bogon IPv6 addresses detected: {total_bogon_ipv6}\n")
        file.write(f"Total unique bogon IPv6 addresses: {len(unique_bogon_ipv6)}\n")

        # Writing detailed country statistics
        for country, stats in country_stats.items():
            file.write(f"\n--- Country: {country} ---\n")
            file.write(f"  Total IPs:\n")
            file.write(f"    IPv4: {stats['total']['ipv4']}, Unique IPv4: {len(stats['total']['unique_ipv4'])}\n")
            file.write(f"    IPv6: {stats['total']['ipv6']}, Unique IPv6: {len(stats['total']['unique_ipv6'])}\n")
            file.write(f"  Source IPs:\n")
            file.write(f"    IPv4: {stats['source']['ipv4']}, Unique IPv4: {len(stats['source']['unique_ipv4'])}\n")
            file.write(f"    IPv6: {stats['source']['ipv6']}, Unique IPv6: {len(stats['source']['unique_ipv6'])}\n")
            file.write(f"  Hop IPs:\n")
            file.write(f"    IPv4: {stats['hop']['ipv4']}, Unique IPv4: {len(stats['hop']['unique_ipv4'])}\n")
            file.write(f"    IPv6: {stats['hop']['ipv6']}, Unique IPv6: {len(stats['hop']['unique_ipv6'])}\n")
            file.write(f"  Destination IPs:\n")
            file.write(f"    IPv4: {stats['destination']['ipv4']}, Unique IPv4: {len(stats['destination']['unique_ipv4'])}\n")
            file.write(f"    IPv6: {stats['destination']['ipv6']}, Unique IPv6: {len(stats['destination']['unique_ipv6'])}\n")
            
            # Path counts for the country
            file.write("  Path Counts:\n")
            file.write(f"    Source Only: {stats['path_counts']['source_only']}\n")
            file.write(f"    Source and Destination: {stats['path_counts']['source_destination']}\n")
            file.write(f"    Source with All Hops and Destination: {stats['path_counts']['source_all_hops_destination']}\n")

            # Boomerang paths
            file.write("    Boomerang Paths:\n")
            for hop_country, boomerang_stats in stats["path_counts"]["boomerang_paths"].items():
                file.write(f"      Through {hop_country}: {boomerang_stats['total']} total\n")
                for traceroute, count in boomerang_stats["per_traceroute"].items():
                    file.write(f"        Traceroute: {traceroute[:50]}... Count: {count}\n")

    # Writing latency data to JSON file
    with open(latency_json_file, "w") as json_file:
        for city_a_id, connections in latency_data.items():
            for city_b_id, stats in connections.items():
                stats["min_latency"] = min(stats["latencies"]) if stats["latencies"] else None
                stats["max_latency"] = max(stats["latencies"]) if stats["latencies"] else None
                stats["average_latency"] = sum(stats["latencies"]) / len(stats["latencies"]) if stats["latencies"] else None
                json.dump({
                    "city_a_id": city_a_id,
                    "city_b_id": city_b_id,
                    "stats": stats,
                    "Country Code": country_code_dict.get(city_a_id, "N/A"),
                    "Longitude": longitude_dict.get(city_a_id, None),
                    "Latitude": latitude_dict.get(city_a_id, None),
                    "ASN": asn_dict.get(city_a_id, "N/A"),
                    "Accuracy Radius": accuracy_radius_dict.get(city_a_id, "N/A"),
                    "Path Count": stats["latency_count"],
                    "Distance_km": distance_dict.get((city_a_id, city_b_id), "N/A")
                }, json_file, indent=4)
                json_file.write("\n")

def write_latency_data_to_json() -> None:
    """
    Writes the latency data between city pairs to a JSON file, including detailed statistics
    like minimum, maximum, average, median, mode latency, and geographic metadata.
    Ensures fields such as Country Code, Longitude, Latitude, ASN, Accuracy Radius, Path Count, 
    AS Relationship, and Distance (in km) are included for each city pair.
    """
    
    # Remove existing JSON file if it exists
    if os.path.exists(latency_json_file):
        os.remove(latency_json_file)

    # Prepare to write latency data in structured JSON format
    with open(latency_json_file, "w") as json_file:
        # Iterate over city pairs and collect latency stats
        for city_a_id, connections in latency_data.items():
            for city_b_id, stats in connections.items():
                # Calculate statistical measures for latencies
                latencies = stats["latencies"]
                if latencies:
                    min_latency = min(latencies)
                    max_latency = max(latencies)
                    average_latency = sum(latencies) / len(latencies)
                    median_latency = calculate_median(latencies)
                    mode_latency = calculate_mode(latencies)
                else:
                    min_latency = max_latency = average_latency = median_latency = mode_latency = None

                # Collect metadata for each city pair from GeoIP and custom dictionaries
                country_code = country_code_dict.get(city_a_id, "N/A")
                longitude = longitude_dict.get(city_a_id, None)
                latitude = latitude_dict.get(city_a_id, None)
                asn = asn_dict.get(city_a_id, "N/A")
                accuracy_radius = accuracy_radius_dict.get(city_a_id, "N/A")
                path_count = stats["latency_count"]
                distance_km = distance_dict.get((city_a_id, city_b_id), "N/A")
                as_relationship = as_relationship_dict.get((city_a_id, city_b_id), "N/A")
                
                # Prepare data for JSON entry
                entry = {
                    "City_A_ID": city_a_id,
                    "City_B_ID": city_b_id,
                    "Country Code": country_code,
                    "Longitude": longitude,
                    "Latitude": latitude,
                    "ASN": asn,
                    "Accuracy Radius": accuracy_radius,
                    "Min Latency": min_latency,
                    "Max Latency": max_latency,
                    "Average Latency": average_latency,
                    "Median Latency": median_latency,
                    "Mode Latency": mode_latency,
                    "Path Count": path_count,
                    "AS Relationship": as_relationship,
                    "Distance_km": distance_km
                }

                # Write the entry to the JSON file
                json.dump(entry, json_file, indent=4)
                json_file.write(",\n")  # Add comma to separate entries in the JSON file

    # Final formatting to ensure valid JSON array structure
    with open(latency_json_file, "r+") as json_file:
        content = json_file.read()
        json_file.seek(0, 0)
        json_file.write("[\n" + content.rstrip(",\n") + "\n]")


def calculate_median(latencies: list[float]) -> Optional[float]:
    """Calculate the median latency from a list of latencies."""
    return median(latencies) if latencies else None

def calculate_mode(latencies: list[float]) -> Optional[float]:
    """Calculate the mode latency from a list of latencies."""
    try:
        return mode(latencies)
    except StatisticsError:
        return None  # In case there is no mode (e.g., all unique values)



def main() -> None:
    """
    Main function to coordinate the process of reading, processing, and recording traceroute data.
    Executes the following steps:
        1. Loads bogon IP sets.
        2. Initializes statistical counters.
        3. Processes each line from the traceroute file.
        4. Calculates and writes final statistics to the output text file.
        5. Saves latency data with geographic and latency statistics to JSON.
    """

    # Logging initialization
    logging.info("Starting traceroute processing workflow.")
    
    # Ensure the statistics and JSON files are removed if they exist
    if os.path.exists(statistics_file):
        os.remove(statistics_file)
    if os.path.exists(latency_json_file):
        os.remove(latency_json_file)

    required_files = [traceroute_file_path, geoip_db_path, bogon_ipv4_path, bogon_ipv6_path]
    for file_path in required_files:
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return

    global bogon_ipv4_set
    global bogon_ipv6_set

    # # Load bogon IP sets from provided files
    bogon_ipv4_set = load_bogon_ips(bogon_ipv4_path)
    bogon_ipv6_set = load_bogon_ips(bogon_ipv6_path)

    logging.info("Bogon IPs loaded.")

    # # Initialize statistical counters and data structures for analysis
    # initialize_counters_and_stats()
    
    # Process traceroute lines
    line_count = 0
    with open(traceroute_file_path, "r") as file:
        for line in tqdm(file, desc="Processing traceroute lines", unit=" lines", total=10000):
            process_traceroute_line(line.strip())
            line_count += 1
            if line_count >= 10000:
                logging.info("Reached 10,000-line processing limit.")
                break


    logging.info("Traceroute processing completed.")
    
    # Write calculated statistics to a text file
    write_statistics_to_file()
    logging.info(f"Statistics written to {statistics_file}.")
    
    # Write latency data to a JSON file
    write_latency_data_to_json()
    logging.info(f"Latency data written to {latency_json_file}.")

    logging.info("Workflow completed successfully.")



if __name__ == "__main__":
    main()