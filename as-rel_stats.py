import os
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor

# Define dataset path
dataset_path = r'E:\Thesis\dataset\20241001.as-rel2.txt'

# Constants
CHUNK_SIZE = 100000  # Read data in chunks to prevent memory overuse

# Function to process each chunk of data
def process_chunk(chunk):
    relationships = []
    unique_as_numbers = set()
    type_counter = Counter()
    degree_counter = defaultdict(int)
    peer_counter = defaultdict(int)

    for line in chunk:
        if line.startswith("#"):
            continue  # Skip comments
        try:
            # Split into up to four parts, but only use the first three
            parts = line.strip().split('|')
            if len(parts) < 3:
                continue  # Skip lines with fewer than three columns
            
            # Extract relevant data
            src, dst, relation = int(parts[0]), int(parts[1]), int(parts[2])

            # Debugging: print processed data (optional for debugging only)
            # print(f"Processed line: {src}|{dst}|{relation}")

            # Update counters
            unique_as_numbers.update([src, dst])
            type_counter[relation] += 1
            degree_counter[src] += 1
            degree_counter[dst] += 1

            # Collect peer relationships for distribution analysis
            if relation == 0:
                peer_counter[src] += 1
                peer_counter[dst] += 1

            # Store relationship
            relationships.append((src, dst, relation))

        except ValueError:
            # Skipping any malformed line
            print(f"Skipping malformed line: {line.strip()}")
            continue

    return relationships, unique_as_numbers, type_counter, degree_counter, peer_counter

# Aggregator function for multi-threaded results
def aggregate_results(results):
    all_relationships = []
    all_unique_as_numbers = set()
    total_type_counter = Counter()
    total_degree_counter = defaultdict(int)
    total_peer_counter = defaultdict(int)

    for relationships, unique_as_numbers, type_counter, degree_counter, peer_counter in results:
        all_relationships.extend(relationships)
        all_unique_as_numbers.update(unique_as_numbers)
        total_type_counter.update(type_counter)
        for asn, degree in degree_counter.items():
            total_degree_counter[asn] += degree
        for asn, peer_count in peer_counter.items():
            total_peer_counter[asn] += peer_count

    return all_relationships, all_unique_as_numbers, total_type_counter, total_degree_counter, total_peer_counter

# Main function to read data in chunks and process with multithreading
def main():
    with open(dataset_path, 'r') as file:
        chunk = []
        results = []

        # Use ThreadPoolExecutor for multithreading
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            while True:
                line = file.readline()
                if not line:
                    break
                chunk.append(line)
                if len(chunk) >= CHUNK_SIZE:
                    # Submit chunk for processing
                    results.append(executor.submit(process_chunk, chunk))
                    chunk = []  # Reset chunk

            # Process any remaining data in the final chunk
            if chunk:
                results.append(executor.submit(process_chunk, chunk))

        # Collect and aggregate results from all threads
        final_results = [r.result() for r in results]
        all_relationships, all_unique_as_numbers, total_type_counter, total_degree_counter, total_peer_counter = aggregate_results(final_results)

    # Summary outputs
    print(f"Total Number of Lines: {len(all_relationships)}")
    print(f"Total Number of Unique ASes: {len(all_unique_as_numbers)}")
    print(f"Type Distribution: {total_type_counter}")
    
    # Degree Distribution Summary
    degree_distribution = sorted(total_degree_counter.items(), key=lambda x: x[1], reverse=True)
    top_as_by_degree = degree_distribution[:10]
    print(f"Top 10 ASes by Degree: {top_as_by_degree}")

    # Peer Relationships Summary
    peer_distribution = sorted(total_peer_counter.items(), key=lambda x: x[1], reverse=True)
    top_as_by_peer_count = peer_distribution[:10]
    print(f"Total Peer Relationships Count: {sum(total_peer_counter.values())}")
    print(f"Top 10 ASes by Peer Count: {top_as_by_peer_count}")

if __name__ == "__main__":
    main()
