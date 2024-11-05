# GeoRoutingAnalysis

This repository contains the Python code and experimental data related to my thesis, **"Analysis of Geography-Oriented Internet Routing."** The project focuses on analyzing the geographic paths that Internet traffic takes and evaluating the implications of these paths on latency, hop-count, and privacy concerns.

## Table of Contents
- [Files](#files)
- [Datasets](#datasets)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Files

The following Python scripts are included in this repository:

- **as-rel_stats.py**: Analyzes the AS relationship statistics for routing data.
- **boomerang.py**: Implements algorithms to identify and analyze boomerang routing paths.
- **boomerang_route_elimination.py**: Eliminates unnecessary boomerang routes from the routing paths.
- **cityMap.py**: Constructs a city map for geographic routing analysis.
- **geographic_avoidance_cost.py**: Evaluates the costs associated with geographic path avoidance.
- **latency_dictionary.py**: Maintains a dictionary for latency data.
- **maxmind_stats.py**: Analyzes statistics from MaxMind GeoIP databases.
- **printDB.py**: Prints the contents of the routing database.
- **routing_data_analysis.py**: Conducts comprehensive analyses of routing data.
- **traceroute_stats.py**: Analyzes traceroute statistics for performance metrics.

## Datasets

The following datasets are included in this repository:

- **20241001.as-rel2.txt**: AS relationship data for routing analysis.
- **GeoIP2-City-Blocks-IPv4.csv**: IPv4 city block data from the GeoIP2 database.
- **GeoIP2-City-Blocks-IPv6.csv**: IPv6 city block data from the GeoIP2 database.
- **GeoIP2-City-Locations-en.csv**: City locations data in English from the GeoIP2 database.
- **GeoIP2-City.mmdb**: MaxMind's GeoIP2 City binary database.
- **GeoLite2-City-Blocks-IPv4.csv**: IPv4 city block data from the GeoLite2 database.
- **GeoLite2-City-Blocks-IPv6.csv**: IPv6 city block data from the GeoLite2 database.
- **GeoLite2-City-Locations-en.csv**: City locations data in English from the GeoLite2 database.
- **GeoLite2-City.mmdb**: MaxMind's GeoLite2 City binary database.
- **latency_data.json**: JSON file containing latency data.
- **latency_data_lite.json**: Output of the statistics from the latency dictionary Python code using GeoLite2.
- **latency_statistics.txt**: Text file summarizing latency statistics.
- **latency_statistics_lite.txt**: Lightweight version of latency statistics in text format.
- **traceroute-2024-10-01T0000**: Traceroute data collected on October 1, 2024.

## Usage

To run the experiments and analyses contained in this repository, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/asolimandev/GeoRoutingAnalysis.git
   cd GeoRoutingAnalysis
