import geoip2.database
import json
from collections import defaultdict
from tqdm import tqdm

# File paths
geoip_db_path = 'E:/internet-graph-master/dataset/GeoIP2-City.mmdb'
traceroute_file_path = "E:/internet-graph-master/dataset/traceroute-2024-10-01T0000"
output_txt_path = "E:/traceroute_africa_country_counts.txt"

# List of African countries (ISO Alpha-2 codes)
AFRICAN_COUNTRIES = {
    "DZ", "AO", "BJ", "BW", "BF", "BI", "CM", "CV", "CF", "TD", "KM", "CG", "CD", 
    "DJ", "EG", "GQ", "ER", "SZ", "ET", "GA", "GM", "GH", "GN", "GW", "CI", "KE", 
    "LS", "LR", "LY", "MG", "MW", "ML", "MR", "MU", "MA", "MZ", "NA", "NE", "NG", 
    "RW", "ST", "SN", "SC", "SL", "SO", "ZA", "SS", "SD", "TZ", "TG", "TN", "UG", 
    "ZM", "ZW"
}

# GDPR countries (ISO Alpha-2 codes)
GDPR_COUNTRIES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", 
    "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"
}

# Initialize GeoIP reader
geoip_reader = geoip2.database.Reader(geoip_db_path)

def get_country_from_ip(ip):
    """
    Get the country code for an IP address using the GeoIP2 database.
    """
    try:
        response = geoip_reader.city(ip)
        country_code = response.country.iso_code
        # Map GDPR countries to a single "GDPR" label
        if country_code in GDPR_COUNTRIES:
            return "GDPR"
        return country_code
    except geoip2.errors.AddressNotFoundError:
        return None

def process_traceroute_file(traceroute_file_path, output_txt_path, african_countries):
    """
    Process the traceroute dataset for specified African countries, separating path and destination country counts.
    """
    country_counts = {
        country: {
            "path_countries": defaultdict(int),
            "destination_countries": defaultdict(int)
        }
        for country in african_countries
    }

    # Read and process the traceroute file line by line
    with open(traceroute_file_path, 'r', encoding='utf-8') as file:
        for i, line in enumerate(tqdm(file, total=10000, desc="Processing traceroutes", unit="lines")):
            if i >= 10000:  # Limit to first 10,000 lines for testing
                break
            try:
                # Parse the line as JSON
                data = json.loads(line.strip())
                src_addr = data.get("src_addr")
                
                # Get the source country
                src_country = get_country_from_ip(src_addr) if src_addr else None
                if src_country in african_countries:
                    # Process hops
                    results = data.get("result", [])
                    for hop in results:
                        hop_results = hop.get("result", [])
                        for hop_data in hop_results:
                            ip = hop_data.get("from")
                            if ip:
                                country = get_country_from_ip(ip)
                                if country:
                                    country_counts[src_country]["path_countries"][country] += 1

                    # Process destination
                    dst_addr = data.get("dst_addr")
                    if dst_addr:
                        country = get_country_from_ip(dst_addr)
                        if country:
                            country_counts[src_country]["destination_countries"][country] += 1
            except Exception as e:
                print(f"Error processing line: {e}")

    # Write all results to a single text file
    with open(output_txt_path, 'w', encoding='utf-8') as txt_file:
        for src_country, counts in country_counts.items():
            txt_file.write(f"Country Counts for Source Country: {src_country}\n\n")
            
            # Write path countries sorted by count
            txt_file.write(f"Path Countries (Sorted):\n")
            txt_file.write(f"{'Country':<20}{'Count':<10}\n")
            txt_file.write(f"{'-'*30}\n")
            sorted_path_countries = sorted(counts["path_countries"].items(), key=lambda x: x[1], reverse=True)
            for country, count in sorted_path_countries:
                txt_file.write(f"{country:<20}{count:<10}\n")
            
            txt_file.write("\n")
            
            # Write destination countries sorted by count
            txt_file.write(f"Destination Countries (Sorted):\n")
            txt_file.write(f"{'Country':<20}{'Count':<10}\n")
            txt_file.write(f"{'-'*30}\n")
            sorted_destination_countries = sorted(counts["destination_countries"].items(), key=lambda x: x[1], reverse=True)
            for country, count in sorted_destination_countries:
                txt_file.write(f"{country:<20}{count:<10}\n")
            
            txt_file.write("\n" + "="*50 + "\n\n")
    print(f"Results written to {output_txt_path}")

if __name__ == "__main__":
    process_traceroute_file(traceroute_file_path, output_txt_path, AFRICAN_COUNTRIES)
    print("Processing completed.")
