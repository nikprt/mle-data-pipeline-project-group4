from pathlib import Path 
from time import time 
from urllib.request import urlretrieve

import pandas as pd
import matplotlib.pyplot as plt

from sqlalchemy import create_engine, inspect, text 
from sqlalchemy.engine import Engine 
import os

_engine: Engine | None = None

ROOT_PATH = Path(__file__).resolve().parent
REPORTS_DIR = ROOT_PATH / "reports"

TABLE_NAME = "green_taxi"
REPORT_QUERY = f"""
    SELECT
        pickup_datetime::date AS pickup_date,
        COUNT(*) AS trips,
        SUM(total_amount) AS revenue
    FROM {TABLE_NAME}
    GROUP BY 1
    ORDER BY 1;
"""

def _database_url() -> str:
    user = os.getenv("PG_USER", "postgres")
    password = os.environ["PG_PASSWORD"]  # no default — fail loudly if missing
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DATABASE", "ny_taxi_green")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

def get_engine() -> Engine:
    """Reuse a single engine/connection pool across all batches instead of creating one per call."""
    
    global _engine
    if _engine is None:
        _engine = create_engine(_database_url())
    return _engine


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

def load_data_batch(df_batch: pd.DataFrame, file_path: str, table_name: str):
    """Load transformed data batch into PostgreSQL database."""
    
    engine = get_engine()
    
    with engine.begin() as conn:
        if inspect(engine).has_table(table_name):
            # re-running this batch shouldn't duplicate rows — clear just this file's rows first
            conn.execute(
                text(f'DELETE FROM "{table_name}" WHERE source_file = :file_path'),
                {"file_path": file_path},
            )
            
    start_time = time()
    df_batch.to_sql(
        table_name,
        engine,
        if_exists="append",     # creates table on first call, appends after
        index=False,
        chunksize=100_000,      # pandas batches INSERT internally - no loop needed here
        method="multi",
    )
    print(f"Loaded {len(df_batch)} rows into '{table_name}' in {time() - start_time:.2f}s")
    

def fetch_daily_revenue(engine: Engine) -> pd.DataFrame:
    """Query the revenue-per-day report from the database."""
    df_report = pd.read_sql(text(REPORT_QUERY), engine)
    df_report["pickup_date"] = pd.to_datetime(df_report["pickup_date"])
    print(f"--> Fetched {len(df_report)} days of report data "
          f"({df_report['pickup_date'].min().date()} to {df_report['pickup_date'].max().date()})")
    return df_report
    
def save_report_csv(df_report: pd.DataFrame, file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    df_report.to_csv(file_path, index=False)
    print(f"--> Saved report CSV to {file_path}")
    
def plot_daily_revenue(df_report: pd.DataFrame, file_path: Path) -> None:
    """Line plot: revenue (USD) per day, dates shown as strings on the x-axis."""
    date_labels = df_report["pickup_date"].dt.strftime("%Y-%m-%d")

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(date_labels, df_report["revenue"], marker="o", markersize=3, linewidth=1)

    ax.set_title(f"Daily Revenue — {TABLE_NAME}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Revenue (USD)")

    # with ~90 daily points, only label every 5th tick to keep the x-axis readable
    step = max(1, len(date_labels) // 20)
    ax.set_xticks(range(0, len(date_labels), step))
    ax.set_xticklabels(date_labels[::step], rotation=90)

    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    file_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(file_path, dpi=150)
    plt.close(fig)
    print(f"--> Saved daily revenue line plot to {file_path}")
    
def plot_revenue_by_weekday(df_report: pd.DataFrame, file_path: Path) -> None:
    """Bar plot: average revenue by day of week — reveals weekly seasonality
    (e.g. weekend vs. weekday demand) that's hard to spot in the daily line plot."""
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    df_weekday = df_report.copy()
    df_weekday["weekday"] = df_weekday["pickup_date"].dt.day_name()
    avg_by_weekday = (
        df_weekday.groupby("weekday")["revenue"]
        .mean()
        .reindex(weekday_order)
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(avg_by_weekday.index, avg_by_weekday.values, color="steelblue")

    ax.set_title(f"Average Revenue by Day of Week — {TABLE_NAME}")
    ax.set_xlabel("Day of Week")
    ax.set_ylabel("Average Revenue (USD)")
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    file_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(file_path, dpi=150)
    plt.close(fig)
    print(f"--> Saved revenue-by-weekday bar plot to {file_path}")

if __name__ == "__main__":

    color_target = "green"
    months_target = [1, 2, 3]
    table_name = f"{color_target}_taxi"
    
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
        load_data_batch(data_batch_transformed, parquet_batch_file_path, table_name)
        
    print(f"Successfully fetched data for the target months (months {months_target}). Creating report now...")    
    
    report_df = fetch_daily_revenue(get_engine())
    save_report_csv(report_df, REPORTS_DIR / f"{TABLE_NAME}_daily_revenue.csv")
    plot_daily_revenue(report_df, REPORTS_DIR / f"{TABLE_NAME}_daily_revenue.png")
    plot_revenue_by_weekday(report_df, REPORTS_DIR / f"{TABLE_NAME}_revenue_by_weekday.png")
        