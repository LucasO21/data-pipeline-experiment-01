# Libraries ----
import pandas as pd
import numpy as np
import requests
import json
import os
from datetime import datetime



  # Retrieve Metadata
base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
url = base_url + "/api/3/action/package_show"
params = {"id": "daily-shelter-overnight-service-occupancy-capacity"}


try:
    package_response = requests.get(url, params = params)
    package_response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX/5XX
    package = package_response.json()
except requests.RequestException as e:
    print(f"Request failed: {e}")
    return df

 # To get resource data:
for resource in package["result"]["resources"]:

    # for datastore_active resources:
    if resource["datastore_active"] and resource["name"] == name:

        # To get all records in CSV format:
        url = base_url + "/datastore/dump/" + resource["id"]
        try:
            df = pd.read_csv(url)
            break  # Exit loop after finding the correct resource
        except pd.errors.ParserError as e:
            print(f"Failed to parse CSV: {e}")
            return df
        except Exception as e:
            print(f"Unexpected error: {e}")
            return df