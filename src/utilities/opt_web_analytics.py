

# Libraries ----
import pandas as pd
import numpy as np
import requests
import json
import zipfile
from io import BytesIO
import aiohttp
import asyncio
from tqdm import tqdm
import hashlib
import pickle
from pathlib import Path
import time
import os
from datetime import datetime


# Function: Get Resource Metadata ----
def get_resource_metadata():
    """
    Retrieves metadata specifically for 'web-analytics-weekly-report' resource
    with detailed status reporting
    """
    # Initialize variables
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    url = base_url + "/api/3/action/package_show"
    params = {"id": "web-analytics"}

    print("\n=== Starting Metadata Retrieval Process ===")

    # Step 1: Initial API Request
    try:
        print("\n1. Making initial API request...")
        package = requests.get(url, params=params)
        package.raise_for_status()
        print("✓ API request successful")
    except requests.RequestException as e:
        print(f"✗ API request failed: {e}")
        return None

    # Step 2: Process API Response
    try:
        print("\n2. Processing API response...")
        package_json = package.json()
        resources = package_json["result"]["resources"]

        if not resources:
            print("✗ No resources found in package")
            return None

        print(f"✓ Found {len(resources)} resources")
    except (KeyError, ValueError) as e:
        print(f"✗ Error processing API response: {e}")
        return None

    # Step 3: Get Metadata for Inactive Resources
    print("\n3. Retrieving resource metadata...")
    resource_metadata_results = []
    target_resource_found = False

    for idx, resource in enumerate(resources, 1):
        if not resource["datastore_active"]:
            print(f"\nProcessing resource {idx}/{len(resources)}: {resource['name']}")

            try:
                metadata_url = f"{base_url}/api/3/action/resource_show?id={resource['id']}"
                metadata_response = requests.get(metadata_url)
                metadata_response.raise_for_status()

                metadata = metadata_response.json()
                if metadata["result"]["name"] == "web-analytics-weekly-report":
                    target_resource_found = True
                    print("✓ Found target resource: web-analytics-weekly-report")
                    resource_metadata_results.append(metadata)

                    # Store zip URL
                    global zip_url  # Make accessible outside function if needed
                    zip_url = metadata["result"]["url"]
                    print("✓ Successfully retrieved zip URL")

            except requests.RequestException as e:
                print(f"✗ Error retrieving metadata for resource {resource['name']}: {e}")
                continue
            except (KeyError, ValueError) as e:
                print(f"✗ Error processing metadata for resource {resource['name']}: {e}")
                continue

    # Step 4: Final Status Report
    print("\n=== Process Complete ===")
    if target_resource_found:
        print("✓ Successfully retrieved metadata for web-analytics-weekly-report")
        print(f"✓ Total metadata records retrieved: {len(resource_metadata_results)}")
        return resource_metadata_results
    else:
        print("✗ Target resource 'web-analytics-weekly-report' not found")
        return None

# # Usage
# if __name__ == "__main__":
#     metadata = get_resource_metadata()
#     if metadata:
#         print("\nMetadata retrieved successfully!")



zip_url = get_resource_metadata()[0]["result"]["url"]

# ! ----
# Cache dictionary to store zip content and timestamps
cache = {}

def process_zip_and_combine_metrics(zip_url, max_folders=5):
    """
    Downloads zip, caches it for 1 hour, lists folders, finds and combines Key Metrics.csv files

    Args:
        zip_url (str): URL of the zip file
        max_folders (int): Maximum number of folders to process (default 5)

    Returns:
        pd.DataFrame: Combined DataFrame of all Key Metrics.csv files
    """
    print("\n=== Starting Process ===")

    # Step 1: Check Cache
    print("\n1. Checking cache...")
    current_time = time.time()
    if zip_url in cache:
        cached_data, timestamp = cache[zip_url]
        if current_time - timestamp < 3600:  # 1 hour cache duration
            print("\u2713 Using cached zip file")
            content = cached_data
        else:
            print("Cache expired. Downloading new zip file...")
            del cache[zip_url]
            content = None
    else:
        content = None

    # Step 2: Download and cache zip file if not cached
    if content is None:
        print("\n2. Downloading zip file...")
        try:
            response = requests.get(zip_url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            content = BytesIO()

            with tqdm(total=total_size, unit='iB', unit_scale=True) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    content.write(chunk)
                    pbar.update(len(chunk))

            print("\u2713 Download complete")
            content.seek(0)
            # Cache the content
            cache[zip_url] = (content, current_time)
        except Exception as e:
            print(f"\u2717 Download error: {e}")
            return pd.DataFrame()

    # Step 3: Process zip contents
    print("\n3. Processing zip contents...")
    try:
        with zipfile.ZipFile(content, "r") as z:
            folders = sorted(
                {os.path.dirname(f.filename) for f in z.filelist if os.path.dirname(f.filename)}
            )[:max_folders]
            print(f"\u2713 Found {len(folders)} folders:")
            for folder in folders:
                print(f"  - {folder}")

            # Step 4: Find and combine Key Metrics files
            print("\n4. Processing Key Metrics files...")
            combined_df = pd.DataFrame()

            for folder in folders:
                metrics_file = next(
                    (f for f in z.filelist
                     if os.path.dirname(f.filename) == folder and os.path.basename(f.filename) == "Key Metrics.csv"),
                    None
                )
                if metrics_file:
                    print(f"\u2713 Found Key Metrics.csv in {folder}")
                    try:
                        with z.open(metrics_file.filename) as f:
                            df = pd.read_csv(f)
                            df['source_folder'] = folder
                            combined_df = pd.concat([combined_df, df], ignore_index=True, sort=False)
                            print(f"  Added {len(df)} rows from {folder}")
                    except Exception as e:
                        print(f"  \u2717 Error reading file from {folder}: {e}")
                else:
                    print(f"- No Key Metrics.csv found in {folder}")

            # Step 5: Return combined results
            if not combined_df.empty:
                print("\n\u2713 Combined data created successfully")
                return combined_df
            else:
                print("\n\u2717 No data was combined - no Key Metrics files found")
                return pd.DataFrame()
    except Exception as e:
        print(f"\u2717 Error processing zip: {e}")
        return pd.DataFrame()

    print("\n=== Process Complete ===")




df = process_zip_and_combine_metrics(zip_url, max_folders=5)
