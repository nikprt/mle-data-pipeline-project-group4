from pathlib import Path 
from time import time 
from urllib.request import urlretrieve

import pandas as pd

ROOT_PATH = Path(__file__).resolve().parent

print(f"ROOT_PATH: {ROOT_PATH}")

def extract_data_from_url(url: str, file_path: str):
    """Read a parquet file from a given URL and write it to a local parquet file."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(url, file_path)
    df_taxi = pd.read_parquet(f"{file_path}")
    return df_taxi



if __name__ == "__main__":

    color_target = "green"
    months_target = [1, 2, 3]
    
    for month_idx in months_target:
        url_source = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color_target}_tripdata_2025-{month_idx:02}.parquet"     
        parquet_batch_file_path = str(ROOT_PATH / "data" / f"{color_target}_tripdate_2025-{month_idx:02}.parquet")
        print(f"Month {month_idx}: URL source of data batch: {url_source}; Parquet Batch File Path: {parquet_batch_file_path}")
        
        # download data batch from url and save as .parquet file
        data_batch = extract_data_from_url(url_source, parquet_batch_file_path)
        print(f"Shape of data batch downloaded = {data_batch.shape}")
        
        