from pathlib import Path 
from time import time 
from urllib.request import urlretrieve

import pandas as pd

ROOT_PATH = Path(__file__).resolve().parent

print(f"ROOT_PATH: {ROOT_PATH}")

def extract_data_batch_from_url(url: str, file_path: str) -> pd.DataFrame:
    """Read a parquet file from a given URL and write it to a local parquet file."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(url, file_path)
    df_batch = pd.read_parquet(f"{file_path}")
    return df_batch

def transform_data_batch(df_batch: pd.DataFrame, file_path: str, color_target: str) -> pd.DataFrame:
    """Transform DataFrame of taxi batch data."""

    df_batch = df_batch.rename(
        columns={
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
            "VendorID": "vendor_id",
            "RatecodeID": "rate_code_id",
            "PULocationID": "pu_location_id",
            "DOLocationID": "do_location_id"
        }
    )
    
    print(f"DataFrame (batch) shape before transformation = {df_batch.shape}")
    
    df_batch["service_type"] = color_target
    df_batch["source_file"] = file_path
    
    # drop impossible trips before they pollute the report
    df_batch = df_batch[
        (df_batch["dropoff_datetime"] > df_batch["pickup_datetime"]) &
        (df_batch["total_amount"] >= 0) &
        (df_batch["trip_distance"] >= 0) &
        (df_batch["passenger_count"] > 0)
    ]
    print(f"DataFrame (batch) shape after transformation = {df_batch.shape}")
    
    int_cols = [
        "vendor_id", 
        "rate_code_id", 
        "pu_location_id", 
        "do_location_id", 
        "passenger_count", 
        "payment_type", 
        "trip_type"
    ]
    
    for col in int_cols:
        df_batch[col] = pd.to_numeric(df_batch[col], errors="coerce").astype("Int64")
        
    # drop column "ehail_fee" as they ~always contain NaN
    df_batch = df_batch.drop(columns=["ehail_fee"])
    
    return df_batch
    

if __name__ == "__main__":

    color_target = "green"
    months_target = [1, 2, 3]
    
    for month_idx in months_target:
        url_source = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color_target}_tripdata_2025-{month_idx:02}.parquet"     
        parquet_batch_file_path = str(ROOT_PATH / "data" / f"{color_target}_tripdate_2025-{month_idx:02}.parquet")
        print(f"--- " * 5)
        print(f"Month {month_idx}: URL source of data batch: {url_source}; Parquet Batch File Path: {parquet_batch_file_path}")
        
        # download data batch from url and save as .parquet file
        data_batch_raw = extract_data_batch_from_url(url_source, parquet_batch_file_path)
        print(f"Shape of data batch downloaded = {data_batch_raw.shape}")
        
        # transform the data
        data_batch_transformed = transform_data_batch(
            data_batch_raw, 
            parquet_batch_file_path, 
            color_target
        )
        
        
        
        
        