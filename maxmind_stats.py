import pandas as pd
import concurrent.futures
import ipaddress
import numpy as np

# File paths
files = {
    'GeoIP2-City-Blocks-IPv4': r'E:\internet-graph-master\dataset\GeoIP2-City-Blocks-IPv4.csv',
    'GeoIP2-City-Blocks-IPv6': r'E:\internet-graph-master\dataset\GeoIP2-City-Blocks-IPv6.csv',
    'GeoLite2-City-Blocks-IPv4': r'E:\internet-graph-master\dataset\GeoLite2-City-Blocks-IPv4.csv',
    'GeoLite2-City-Blocks-IPv6': r'E:\internet-graph-master\dataset\GeoLite2-City-Blocks-IPv6.csv',
    'GeoIP2-City-Locations': r'E:\internet-graph-master\dataset\GeoIP2-City-Locations-en.csv',
    'GeoLite2-City-Locations': r'E:\internet-graph-master\dataset\GeoLite2-City-Locations-en.csv'
}

def load_csv(file):
    return pd.read_csv(file, dtype={'network': str, 'geoname_id': str, 'registered_country_geoname_id': str,
                                    'represented_country_geoname_id': str, 'is_anonymous_proxy': str,
                                    'is_satellite_provider': str, 'postal_code': str, 'latitude': float,
                                    'longitude': float, 'accuracy_radius': float, 'is_anycast': str})

def analyze_network_data(file):
    df = load_csv(file)
    df.dropna(subset=['network'], inplace=True)
    
    unique_networks = df['network'].nunique()
    cidr_lengths = df['network'].apply(lambda net: ipaddress.ip_network(net, strict=False).prefixlen)
    distribution_stats = {
        'mean': cidr_lengths.mean(),
        'median': cidr_lengths.median(),
        'std': cidr_lengths.std()
    }
    return {
        'unique_networks': unique_networks,
        'cidr_length_stats': distribution_stats
    }

def analyze_geoname_data(file):
    df = load_csv(file)
    
    unique_geoname_ids = df['geoname_id'].nunique()
    unique_registered_vs_represented = df.groupby(['registered_country_geoname_id', 'represented_country_geoname_id']).ngroups
    proxy_counts = df['is_anonymous_proxy'].value_counts().get('1', 0)
    satellite_counts = df['is_satellite_provider'].value_counts().get('1', 0)
    accuracy_stats = {
        'mean': df['accuracy_radius'].mean(),
        'median': df['accuracy_radius'].median(),
        'std': df['accuracy_radius'].std()
    }
    return {
        'unique_geoname_ids': unique_geoname_ids,
        'registered_vs_represented_pairs': unique_registered_vs_represented,
        'proxy_counts': proxy_counts,
        'satellite_counts': satellite_counts,
        'accuracy_radius_stats': accuracy_stats
    }

def analyze_location_data(file):
    df = load_csv(file)
    
    country_distribution = df['country_iso_code'].value_counts()
    continent_distribution = df['continent_code'].value_counts()
    city_within_country = df.groupby('country_iso_code')['city_name'].nunique()
    subdivision_distribution = df['subdivision_1_iso_code'].value_counts()
    time_zone_distribution = df['time_zone'].value_counts()
    eu_membership_percentage = (df['is_in_european_union'] == '1').mean() * 100
    
    return {
        'country_distribution': country_distribution,
        'continent_distribution': continent_distribution,
        'city_within_country_counts': city_within_country,
        'subdivision_distribution': subdivision_distribution,
        'time_zone_distribution': time_zone_distribution,
        'eu_membership_percentage': eu_membership_percentage
    }

def main_comparison():
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(analyze_network_data, files['GeoIP2-City-Blocks-IPv4']): 'GeoIP2-IPv4-Network',
            executor.submit(analyze_network_data, files['GeoIP2-City-Blocks-IPv6']): 'GeoIP2-IPv6-Network',
            executor.submit(analyze_network_data, files['GeoLite2-City-Blocks-IPv4']): 'GeoLite2-IPv4-Network',
            executor.submit(analyze_network_data, files['GeoLite2-City-Blocks-IPv6']): 'GeoLite2-IPv6-Network',
            executor.submit(analyze_geoname_data, files['GeoIP2-City-Blocks-IPv4']): 'GeoIP2-IPv4-Geoname',
            executor.submit(analyze_geoname_data, files['GeoIP2-City-Blocks-IPv6']): 'GeoIP2-IPv6-Geoname',
            executor.submit(analyze_geoname_data, files['GeoLite2-City-Blocks-IPv4']): 'GeoLite2-IPv4-Geoname',
            executor.submit(analyze_geoname_data, files['GeoLite2-City-Blocks-IPv6']): 'GeoLite2-IPv6-Geoname',
            executor.submit(analyze_location_data, files['GeoIP2-City-Locations']): 'GeoIP2-Location',
            executor.submit(analyze_location_data, files['GeoLite2-City-Locations']): 'GeoLite2-Location'
        }

        for future in concurrent.futures.as_completed(futures):
            result_name = futures[future]
            try:
                results[result_name] = future.result()
            except Exception as e:
                print(f"Error processing {result_name}: {e}")

    return results

if __name__ == "__main__":
    comparison_results = main_comparison()
    for key, result in comparison_results.items():
        print(f"Results for {key}:\n")
        for metric, value in result.items():
            print(f"  {metric}: {value}")
        print("\n")
