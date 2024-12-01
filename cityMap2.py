import csv
import json
from statistics import median
import math

def calculate_95th_percentile(latencies):
    """
    Calculate the 95th percentile of a list of latencies.
    """
    if not latencies:
        return None
    sorted_latencies = sorted(latencies)
    index = math.ceil(0.95 * len(sorted_latencies)) - 1
    return sorted_latencies[index]

def process_json_to_csv(json_file_path, csv_file_path):
    """
    Process the JSON file and write latency metrics to a CSV file.
    Args:
        json_file_path (str): Path to the JSON file containing latency data.
        csv_file_path (str): Path to the output CSV file.
        max_lines (int): Maximum number of lines to write to the CSV file.
    """
    # Open the JSON file
    with open(json_file_path, "r", encoding="utf-8") as json_file:
        latency_data = json.load(json_file)

    # Open the CSV file for writing
    with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC)

        # Write the header row
        csv_writer.writerow(["City1_ID", "City2_ID", "Min_Latency", "Median_Latency", "95th_Percentile_Latency"])
        # Iterate over city pairs and calculate metrics
        for city1_id, connections in latency_data.items():
            for city2_id, stats in connections.items():
                latencies = stats.get("latencies", [])
                if latencies:
                    min_latency = round(min(latencies), 4)
                    median_latency = round(median(latencies), 4)
                    percentile_95 = round(calculate_95th_percentile(latencies), 4)

                    # Write the row to the CSV file without pre-applying quotes
                    csv_writer.writerow([city1_id, city2_id, min_latency, median_latency, percentile_95])

# Paths to input JSON file and output CSV file
json_file_path = "E:/latency_data3.json"  # Replace with your actual JSON file path
csv_file_path = "E:/cityMap.csv"  # Replace with your desired output CSV file path

# Process the JSON file and write the CSV output
process_json_to_csv(json_file_path, csv_file_path)
