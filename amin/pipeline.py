"""
ELT pipeline for the Green Taxi project: extract to files, load into DuckDB, transform with SQL.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import click
import duckdb
import requests

MONTHS = ("2025-01", "2025-02", "2025-03")
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

AMIN_ROOT = Path(__file__).resolve().parent
RAW_DIR = AMIN_ROOT / "data" / "raw"
PROCESSED_DIR = AMIN_ROOT / "data" / "processed"
DB_PATH = AMIN_ROOT / "data" / "green_taxi.duckdb"

QUARTER_START = "2025-01-01"
QUARTER_END = "2025-04-01"

DAILY_REVENUE_SQL = f"""
CREATE OR REPLACE TABLE daily_revenue AS
SELECT
    CAST(lpep_pickup_datetime AS DATE) AS service_date,
    COUNT(*) AS trips,
    ROUND(SUM(total_amount), 2) AS revenue,
    ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip
FROM raw_trips
WHERE lpep_pickup_datetime >= TIMESTAMP '{QUARTER_START}'
  AND lpep_pickup_datetime < TIMESTAMP '{QUARTER_END}'
GROUP BY 1
ORDER BY 1
"""


def filename(month):
    return f"green_tripdata_{month}.parquet"


def extract(months=MONTHS, raw_dir=RAW_DIR, force=False):
    """
    Download one Parquet file per month into the staging folder.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for month in months:
        target = raw_dir / filename(month)
        if target.exists() and not force:
            click.echo(f"reused {target.name}")
        else:
            part = target.with_suffix(".part")
            with requests.get(f"{BASE_URL}/{filename(month)}", stream=True, timeout=60) as response:
                response.raise_for_status()
                with part.open("wb") as file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        file.write(chunk)
            part.replace(target)
            click.echo(f"downloaded {target.name}")
        paths.append(target)
    return paths


def connect(db_path=DB_PATH):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(db_path)
    connection.execute("SET threads = 2")
    connection.execute("SET memory_limit = '1GB'")
    return connection


def load(connection, paths):
    """
    Load the staged files into DuckDB as they are, no cleaning yet.
    """
    files = [str(path) for path in paths]
    connection.execute("CREATE OR REPLACE TABLE raw_trips AS SELECT * FROM read_parquet($files)", {"files": files})
    rows = connection.execute("SELECT COUNT(*) FROM raw_trips").fetchone()[0]
    click.echo(f"loaded {rows:,} rows into raw_trips")
    return rows


def transform(connection):
    """
    Build the daily revenue table inside the database and export it.
    """
    connection.execute(DAILY_REVENUE_SQL)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = PROCESSED_DIR / "daily_revenue.csv"
    parquet_path = PROCESSED_DIR / "daily_revenue.parquet"
    connection.execute(f"COPY daily_revenue TO '{csv_path.as_posix()}' (HEADER, DELIMITER ',')")
    connection.execute(f"COPY daily_revenue TO '{parquet_path.as_posix()}' (FORMAT PARQUET)")

    rows_in, days, trips, revenue, first_day, last_day = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM raw_trips),
            COUNT(*), SUM(trips), ROUND(SUM(revenue), 2), MIN(service_date), MAX(service_date)
        FROM daily_revenue
        """
    ).fetchone()
    metadata = {
        "run_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "months": list(MONTHS),
        "rows_loaded": rows_in,
        "rows_outside_quarter": rows_in - trips,
        "days": days,
        "trips": trips,
        "revenue_total": float(revenue),
        "first_day": str(first_day),
        "last_day": str(last_day),
    }
    metadata_path = PROCESSED_DIR / "run_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    click.echo(f"wrote {csv_path.name}, {parquet_path.name} and run_metadata.json")
    click.echo(f"summary: days={days}, trips={trips:,}, revenue={revenue:,.2f}")
    return metadata


@click.command()
@click.option("--step", type=click.Choice(["extract", "load", "transform", "all"]), default="all", show_default=True)
@click.option("--force", is_flag=True, help="Download again even if the file is already staged.")
def main(step, force):
    """
    Run the Green Taxi ELT pipeline.
    """
    paths = [RAW_DIR / filename(month) for month in MONTHS]
    if step in ("extract", "all"):
        paths = extract(force=force)
    if step in ("load", "transform", "all"):
        if step in ("load", "all"):
            missing = [path.name for path in paths if not path.exists()]
            if missing:
                raise click.ClickException(f"missing staged files: {', '.join(missing)}. Run --step extract first.")
        connection = connect()
        try:
            if step in ("load", "all"):
                load(connection, paths)
            if step in ("transform", "all"):
                transform(connection)
        finally:
            connection.close()


if __name__ == "__main__":
    main()
