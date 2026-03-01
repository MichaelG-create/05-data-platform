"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: extracted_at
    type: timestamp
    description: "Time of extraction"
  - name: taxi_type
    type: string
    description: "Type of taxi"

@bruin"""

import pandas as pd
import requests
from io import BytesIO
import json
import os
from dateutil.relativedelta import relativedelta
from datetime import datetime

def materialize():
    start_date_str = os.environ.get("BRUIN_START_DATE")
    end_date_str = os.environ.get("BRUIN_END_DATE")
    
    if not start_date_str or not end_date_str:
        print("Missing start or end date.")
        return pd.DataFrame()
        
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    dataframes = []
    
    current_date = start_date.replace(day=1)
    end_month_date = end_date.replace(day=1)
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    while current_date <= end_month_date:
        year_month = current_date.strftime("%Y-%m")
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year_month}.parquet"
            url = f"{base_url}/{filename}"
            print(f"Fetching: {url}")
            
            response = requests.get(url)
            if response.status_code == 200:
                df = pd.read_parquet(BytesIO(response.content))
                # standardize columns from both types slightly if needed, but keeping raw
                if taxi_type == 'green':
                    # Sometimes green has lpep_pickup_datetime and yellow has tpep_pickup_datetime
                    # Staging can handle column renames. Just append taxi metadata
                    pass
                df["taxi_type"] = taxi_type
                df["extracted_at"] = pd.Timestamp.now()
                dataframes.append(df)
            else:
                print(f"Failed to fetch {url}: Status {response.status_code}")
                
        current_date += relativedelta(months=1)
        
    if not dataframes:
        return pd.DataFrame()
        
    final_dataframe = pd.concat(dataframes, ignore_index=True)
    return final_dataframe
