import json
import numpy as np
from collections import Counter
from tqdm import tqdm

# File path (update if needed)
file_path = r'E:\internet-graph-master\dataset\traceroute-2024-10-01T0000'

# Initialize metrics
unique_probe_ids = set()
unique_dst_ips = Counter()
responding_destinations = 0
non_responding_destinations = 0
hop_counts = []
rtt_values = []
mpls_counts = 0
proto_usage = Counter()
version_distribution = Counter()
ttl_values = []
total_lines = 0  # Track total lines processed
skipped_lines = 0  # Track lines with JSON decoding errors

# Process each line individually
with open(file_path, 'r') as file:
    for line in tqdm(file, desc="Processing dataset"):
        total_lines += 1
        try:
            # Load JSON line
            data = json.loads(line)

            # Unique Probe IDs and Destination IPs
            unique_probe_ids.add(data.get('prb_id'))
            dst_ip = data.get('dst_addr')
            unique_dst_ips[dst_ip] += 1
            
            # Check if destination responded
            if data.get("destination_ip_responded"):
                responding_destinations += 1
            else:
                non_responding_destinations += 1

            # Protocol and version
            proto_usage[data.get('proto')] += 1
            version_distribution[data.get('mver')] += 1

            # Hop and RTT Analysis
            hop_data = data.get('result', [])
            hop_counts.append(len(hop_data))
            for hop in hop_data:
                for result in hop.get('result', []):
                    # RTT values for responsive hops
                    if 'rtt' in result:
                        rtt_values.append(result['rtt'])
                    
                    # MPLS Labeled Hops
                    icmpext = result.get('icmpext')
                    if icmpext and 'mpls' in icmpext.get('obj', [{}])[0]:
                        mpls_counts += 1
                    
                    # TTL values
                    if 'ttl' in result:
                        ttl_values.append(result['ttl'])

        except json.JSONDecodeError:
            skipped_lines += 1  # Count skipped lines if JSON fails

# Safely calculate final metrics, handling zero division
total_measurements = len(hop_counts)
responding_percentage = (responding_destinations / total_measurements) * 100 if total_measurements else 0
non_responding_percentage = (non_responding_destinations / total_measurements) * 100 if total_measurements else 0
avg_hops_per_measurement = np.mean(hop_counts) if hop_counts else 0
mpls_percentage = (mpls_counts / sum(hop_counts)) * 100 if hop_counts else 0

# Final calculations for RTT if available
average_rtt = np.mean(rtt_values) if rtt_values else 0
median_rtt = np.median(rtt_values) if rtt_values else 0
max_rtt = np.max(rtt_values) if rtt_values else 0
min_rtt = np.min(rtt_values) if rtt_values else 0
rtt_variability = (np.std(rtt_values), np.percentile(rtt_values, 75) - np.percentile(rtt_values, 25)) if rtt_values else (0, 0)

# Collect metrics
metrics = {
    "Total Measurements": total_measurements,
    "Total Lines Processed": total_lines,
    "Skipped Lines (JSON Decode Error)": skipped_lines,
    "Distinct Probe IDs": len(unique_probe_ids),
    "Unique Destination IPs": len(unique_dst_ips),
    "Top Destination IPs": unique_dst_ips.most_common(5),
    "Responding Destinations %": responding_percentage,
    "Non-Responding Destinations %": non_responding_percentage,
    "Average Hops per Measurement": avg_hops_per_measurement,
    "Hop Count Distribution (Median, StdDev)": (np.median(hop_counts), np.std(hop_counts)) if hop_counts else (0, 0),
    "MPLS Labeled Hops %": mpls_percentage,
    "Average RTT": average_rtt,
    "Median RTT": median_rtt,
    "Max RTT": max_rtt,
    "Min RTT": min_rtt,
    "RTT Variability (StdDev, IQR)": rtt_variability,
    "Protocol Distribution": proto_usage,
    "Version Distribution": version_distribution,
    "TTL Summary (Mean, Median)": (np.mean(ttl_values), np.median(ttl_values)) if ttl_values else (0, 0)
}

# Display results
for key, value in metrics.items():
    print(f"{key}: {value}")
