import geoip2.database
import logging
import json
from collections import defaultdict
from tqdm import tqdm
import ipaddress
import os

# File paths
statistics_file = "latency_statistics3.txt"
latency_json_file = "latency_data3.json"
geoip_db_path = 'E:/internet-graph-master/dataset/GeoIP2-City.mmdb'
traceroute_file_path = "E:/internet-graph-master/dataset/traceroute-2024-10-01T0000"

# List of GDPR countries
gdpr_countries = {
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", 
    "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"
}

# Remove output files if they exist
if os.path.exists(statistics_file):
    os.remove(statistics_file)
if os.path.exists(latency_json_file):
    os.remove(latency_json_file)

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

# GeoIP database reader
geoip_reader = geoip2.database.Reader(geoip_db_path)

# Initialize counters and sets for statistics
total_ip_addresses = 0
unique_ip_addresses = set()
total_latencies = 0
unique_cities = set()
unique_countries = set()
unique_subdivisions = set()
missing_city_name_counter = 0
missing_country_counter = 0
private_or_cgnat_ip_counter = 0

ipv4_count = 0
ipv6_count = 0
unique_ipv4 = set()
unique_ipv6 = set()

# Specific counters for source, hop, and destination IPs
source_ipv4_count = 0
source_ipv6_count = 0
hop_ipv4_count = 0
hop_ipv6_count = 0
destination_ipv4_count = 0
destination_ipv6_count = 0

# Country-specific statistics, treating GDPR countries as one entity
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

# Dictionary to hold latencies for each city pair
latency_data = defaultdict(lambda: defaultdict(lambda: {"latency_count": 0, "latencies": []}))

def is_private_or_cgnat_ip(ip):
    """Check if the IP address is within a private or CGNAT range."""
    try:
        ip_obj = ipaddress.ip_address(ip)
        return ip_obj.is_private or ip_obj in ipaddress.ip_network("100.64.0.0/10")
    except ValueError:
        return False

def get_country_label(country_code):
    """Returns 'GDPR' if the country is in the GDPR list, otherwise returns the country code."""
    return "GDPR" if country_code in gdpr_countries else country_code

def city_id_from_ip(ip):
    """Get city_id based on IP using the GeoIP database."""
    global missing_city_name_counter, missing_country_counter, private_or_cgnat_ip_counter
    if is_private_or_cgnat_ip(ip):
        private_or_cgnat_ip_counter += 1
        return None, None

    try:
        response = geoip_reader.city(ip)
        city = response.city.name
        subdivision = response.subdivisions.most_specific.name
        country = response.country.iso_code

        if not city:
            missing_city_name_counter += 1
        if not country:
            missing_country_counter += 1

        if city and country:
            unique_cities.add(city)
            country_label = get_country_label(country)
            unique_countries.add(country_label)
            if subdivision:
                unique_subdivisions.add(subdivision)
            return f"{city}#{subdivision}#{country_label}", country_label
    except geoip2.errors.AddressNotFoundError:
        pass
    return None, None

def update_ip_stats(ip, category, country=None):
    """Classify and count IP addresses by type and track statistics."""
    global total_ip_addresses, ipv4_count, ipv6_count
    global source_ipv4_count, source_ipv6_count, hop_ipv4_count, hop_ipv6_count, destination_ipv4_count, destination_ipv6_count

    try:
        ip_obj = ipaddress.ip_address(ip)
        total_ip_addresses += 1

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

        unique_ip_addresses.add(ip)
    except ValueError:
        logging.error(f"Invalid IP address encountered: {ip}")


def calculate_latency(rtt):
    """Calculate latency as half the round-trip time (RTT)."""
    return rtt / 2.0

def process_traceroute_line(line):
    """Process each traceroute line to extract city-to-city latency data."""
    global total_latencies

    try:
        data = json.loads(line)
        src_addr = data.get("src_addr")
        dst_addr = data.get("dst_addr")
        results = data.get("result", [])

        if src_addr:
            city_a_id, src_country = city_id_from_ip(src_addr)
            if city_a_id:
                update_ip_stats(src_addr, "source", src_country)
        else:
            city_a_id, src_country = None, None

        hop_countries = set()
        path_countries = []
        source_to_dest = False

        for hop in results:
            hop_results = hop.get("result", [])

            for hop_data in hop_results:
                from_ip = hop_data.get("from")
                rtt = hop_data.get("rtt")

                if from_ip and rtt is not None:
                    city_b_id, hop_country = city_id_from_ip(from_ip)
                    if city_b_id:
                        update_ip_stats(from_ip, "hop", hop_country)
                        if hop_country and hop_country != src_country:
                            hop_countries.add(hop_country)
                        path_countries.append(hop_country)

                    if city_a_id and city_b_id and city_a_id != city_b_id:
                        latency = calculate_latency(rtt)
                        if latency != 0.0 :
                            latency_data[city_a_id][city_b_id]["latencies"].append(latency)
                            latency_data[city_a_id][city_b_id]["latency_count"] += 1
                            total_latencies += 1
                            city_a_id = city_b_id

        if dst_addr:
            dst_city_id, dst_country = city_id_from_ip(dst_addr)
            if dst_city_id:
                update_ip_stats(dst_addr, "destination", dst_country)
            if city_a_id and dst_city_id and city_a_id != dst_city_id:
                avg_rtt = sum(hop_data.get("rtt", 0) for hop_data in hop_results if hop_data.get("rtt") is not None) / max(len(hop_results), 1)
                latency = calculate_latency(avg_rtt)
                if latency != 0.0 :
                    latency_data[city_a_id][dst_city_id]["latencies"].append(latency)
                    latency_data[city_a_id][dst_city_id]["latency_count"] += 1
                    total_latencies += 1

            source_to_dest = src_country == dst_country
            if src_country:
                if source_to_dest:
                    country_stats[src_country]["path_counts"]["source_destination"] += 1
                if dst_country == src_country and hop_countries:
                    for hop_country in hop_countries:
                        country_stats[src_country]["path_counts"]["boomerang_paths"][hop_country]["total"] += 1
                        country_stats[src_country]["path_counts"]["boomerang_paths"][hop_country]["per_traceroute"][line] += 1
        if src_country:
            if not hop_countries:
                country_stats[src_country]["path_counts"]["source_only"] += 1
            if hop_countries and dst_country:
                for hop_country in hop_countries:
                    if hop_country and hop_country != src_country:
                        country_stats[src_country]["path_counts"]["source_with_other_country"][hop_country] += 1
    except Exception as e:
        logging.error(f"Error processing line: {e}")

def main():
    logging.info("Starting traceroute processing")

    with open(traceroute_file_path, "r") as file:
        for i, line in enumerate(tqdm(file, total=8706125, desc="Processing traceroute lines", unit=" lines")):
            process_traceroute_line(line)

    # Write statistics to the text file
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
            
            file.write("  Path Counts:\n")
            file.write(f"    Source Only: {stats['path_counts']['source_only']}\n")
            file.write(f"    Source and Destination: {stats['path_counts']['source_destination']}\n")
            file.write(f"    Source with All Hops and Destination: {stats['path_counts']['source_all_hops_destination']}\n")
            file.write("    Boomerang Paths:\n")
            for hop_country, boomerang_stats in stats["path_counts"]["boomerang_paths"].items():
                file.write(f"      Through {hop_country}: {boomerang_stats['total']} total\n")
                file.write("      Per Traceroute:\n")
                for traceroute, count in boomerang_stats["per_traceroute"].items():
                    file.write(f"        Traceroute {traceroute}: {count}\n")

    with open(latency_json_file, "w") as json_file:
        json.dump(latency_data, json_file, indent=4)

if __name__ == "__main__":
    main()
