import json
import csv
import numpy as np
from tqdm import tqdm
import geoip2.database
import networkx as nx

# File paths
geoip_db_path = 'E:/internet-graph-master/dataset/GeoIP2-City.mmdb'
traceroute_file_path = "E:/internet-graph-master/dataset/traceroute-2024-10-01T0000"
city_map_path = "E:/cityMap.csv"
output_file_path = "E:/australia_Indonesia_analysis4.txt"

SOURCE_JURISDICTION = "AU"  # Source jurisdiction to analyze
AVOID_JURISDICTION = "ID"   # Jurisdiction to avoid in paths
MAX_TRACEROUTES = 9000000    # Limit on the number of traceroutes to process


# Initialize data structures for results
latencies = {"min": [], "median": [], "95th": []}
avoidance_latencies = {"min": [], "median": [], "95th": []}
city_hops = {"min": [], "median": [], "95th": []}
jurisdiction_hops = {"min": [], "median": [], "95th": []}
avoidance_city_hops = {"min": [], "median": [], "95th": []}
avoidance_jurisdiction_hops = {"min": [], "median": [], "95th": []}
total_paths = 0
alternative_paths_count = 0

def city_id_from_ip(ip, geoip_reader):
    """Get the city ID and country from an IP address using MaxMind."""
    if not ip:  # Ensure the IP address is not empty
        return None, None
    try:
        response = geoip_reader.city(ip)
        city = response.city.name
        subdivision = response.subdivisions.most_specific.name
        country = response.country.iso_code

        if city and subdivision and country:
            city_id = f"{city}#{subdivision}#{country}"
            return city_id, country
        return None, None
    except geoip2.errors.AddressNotFoundError:
        return None, None
    except ValueError:
        # Handle invalid IPs passed to geoip_reader.city()
        return None, None

def calculate_statistics(data):
    """Compute statistical metrics for the given data."""
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

def load_city_map(file_path):
    """Load city map from CSV into separate latency graphs."""
    graph_min = nx.DiGraph()
    graph_median = nx.DiGraph()
    graph_95th = nx.DiGraph()
    with open(file_path, 'r', encoding='utf-8') as city_file:
        csv_reader = csv.reader(city_file)
        next(csv_reader)  # Skip header
        for row in csv_reader:
            src, dst, min_latency, median_latency, p95_latency = row
            graph_min.add_edge(src, dst, weight=float(min_latency))
            graph_median.add_edge(src, dst, weight=float(median_latency))
            graph_95th.add_edge(src, dst, weight=float(p95_latency))
    return graph_min, graph_median, graph_95th

from collections import Counter

# Track alternative countries
alternative_countries = Counter()

def find_shortest_path_avoiding_chile(hops, graph, chile_nodes, geoip_reader):
    """Find the shortest path avoiding Chile nodes."""
    modified_graph = graph.copy()
    modified_graph.remove_nodes_from(chile_nodes)

    path_latencies = []
    try:
        for i in range(len(hops) - 1):
            src, dst = hops[i], hops[i + 1]
            if src not in modified_graph or dst not in modified_graph:
                continue
            shortest_path = nx.shortest_path(modified_graph, source=src, target=dst, weight='weight')
            path_latencies.extend([graph[src][neighbor]['weight'] for src, neighbor in zip(shortest_path[:-1], shortest_path[1:])])
            


            # Track alternative countries used in the path
            for hop in shortest_path:
                country_code = hop.split("#")[-1]  # Extract the country code
                alternative_countries[country_code] += 1

    except (nx.NetworkXNoPath, KeyError):
        return []
    return path_latencies

# Load city map
graph_min, graph_median, graph_95th = load_city_map(city_map_path)



# Main analysis
with geoip2.database.Reader(geoip_db_path) as geoip_reader, \
     open(traceroute_file_path, 'r', encoding='ISO-8859-1') as traceroute_file, \
     open(output_file_path, 'w', encoding='utf-8') as output_file:

    output_file.write("Brazil -> Chile Analysis\n\n")

    for line_num, line in enumerate(tqdm(traceroute_file, desc="Processing traceroutes")):
        if line_num >= MAX_TRACEROUTES:
            break  # Stop after processing MAX_TRACEROUTES

        try:
            data = json.loads(line)
            src_addr = data.get("src_addr", "")
            dst_addr = data.get("dst_addr", "")
            hops = data.get("result", [])

            src_city_id, src_country = city_id_from_ip(src_addr, geoip_reader)
            if src_country != SOURCE_JURISDICTION:
                continue

            path_hops = []
            path_jurisdictions = []
            chile_nodes = []
            for hop in hops:
                for result in hop.get("result", []):
                    hop_ip = result.get("from", "")
                    hop_city_id, hop_country = city_id_from_ip(hop_ip, geoip_reader)
                    if hop_city_id:
                        path_hops.append(hop_city_id)
                        path_jurisdictions.append(hop_country)
                        if hop_country == AVOID_JURISDICTION:
                            chile_nodes.append(hop_city_id)

            if not path_hops:
                continue

            # Add hop counts
            for key in city_hops.keys():
                city_hops[key].append(len(set(path_hops)))
                jurisdiction_hops[key].append(len(set(path_jurisdictions)))


            if chile_nodes:
                total_paths += 1
                # Calculate original latencies
                latencies["min"].extend([graph_min[src][dst]['weight'] for src, dst in zip(path_hops[:-1], path_hops[1:]) if graph_min.has_edge(src, dst)])
                latencies["median"].extend([graph_median[src][dst]['weight'] for src, dst in zip(path_hops[:-1], path_hops[1:]) if graph_median.has_edge(src, dst)])
                latencies["95th"].extend([graph_95th[src][dst]['weight'] for src, dst in zip(path_hops[:-1], path_hops[1:]) if graph_95th.has_edge(src, dst)])


                # Alternative latencies
                avoidance_latencies["min"].extend(find_shortest_path_avoiding_chile(path_hops, graph_min, chile_nodes, geoip_reader))
                avoidance_latencies["median"].extend(find_shortest_path_avoiding_chile(path_hops, graph_median, chile_nodes, geoip_reader))
                avoidance_latencies["95th"].extend(find_shortest_path_avoiding_chile(path_hops, graph_95th, chile_nodes, geoip_reader))

                alternative_paths_count += 1

                # Alternative hops
                modified_path_hops = [hop for hop in path_hops if hop not in chile_nodes]
                modified_path_jurisdictions = [country for hop, country in zip(path_hops, path_jurisdictions) if hop not in chile_nodes]
                for key in avoidance_city_hops.keys():
                    avoidance_city_hops[key].append(len(set(modified_path_hops)))
                    avoidance_jurisdiction_hops[key].append(len(set(modified_path_jurisdictions)))

            

        except json.JSONDecodeError:
            continue

    # Compute statistics
    stats = {
        "Min Latency Statistics": calculate_statistics(latencies["min"]),
        "Min Latency Statistics After Avoidance": calculate_statistics(avoidance_latencies["min"]),
        "Median Latency Statistics": calculate_statistics(latencies["median"]),
        "Median Latency Statistics After Avoidance": calculate_statistics(avoidance_latencies["median"]),
        "95th Percentile Latency Statistics": calculate_statistics(latencies["95th"]),
        "95th Percentile Latency Statistics After Avoidance": calculate_statistics(avoidance_latencies["95th"]),
    }

    hop_stats = {
        "Min City Hops Statistics": calculate_statistics(city_hops["min"]),
        "Min City Hops Statistics After Avoidance": calculate_statistics(avoidance_city_hops["min"]),
        "Min Jurisdiction Hops Statistics": calculate_statistics(jurisdiction_hops["min"]),
        "Min Jurisdiction Hops Statistics After Avoidance": calculate_statistics(avoidance_jurisdiction_hops["min"]),
        "Median City Hops Statistics": calculate_statistics(city_hops["median"]),
        "Median City Hops Statistics After Avoidance": calculate_statistics(avoidance_city_hops["median"]),
        "Median Jurisdiction Hops Statistics": calculate_statistics(jurisdiction_hops["median"]),
        "Median Jurisdiction Hops Statistics After Avoidance": calculate_statistics(avoidance_jurisdiction_hops["median"]),
        "95th Percentile City Hops Statistics": calculate_statistics(city_hops["95th"]),
        "95th Percentile City Hops Statistics After Avoidance": calculate_statistics(avoidance_city_hops["95th"]),
        "95th Percentile Jurisdiction Hops Statistics": calculate_statistics(jurisdiction_hops["95th"]),
        "95th Percentile Jurisdiction Hops Statistics After Avoidance": calculate_statistics(avoidance_jurisdiction_hops["95th"]),
    }

    # Write results
    output_file.write(f"Total paths with Brazil as source and Chile as hop: {total_paths}\n")
    output_file.write(f"Paths with alternative avoiding Chile: {alternative_paths_count}\n\n")

    for title, stat in stats.items():
        output_file.write(f"{title}:\n{stat}\n\n")

    for title, stat in hop_stats.items():
        output_file.write(f"{title}:\n{stat}\n\n")

    # Write top 5 alternative countries
    top_alternative_countries = alternative_countries.most_common(5)
    output_file.write("Top 5 alternative countries:\n")
    for country, count in top_alternative_countries:
        output_file.write(f"{country}: {count}\n")
