# Amin's pipeline: Green Taxi daily revenue

My version of the project. It is an **ELT** pipeline on DuckDB: Python downloads the files and
puts them into a local database as they are, and SQL does the rest.

## Run it

```bash
uv run python amin/pipeline.py                 # extract, load, transform
uv run python amin/pipeline.py --step extract  # only the download
uv run pytest amin/tests                       # 3 tests, no download needed
```

Bonus, the same pipeline as a Prefect flow:

```bash
uv run prefect config unset PREFECT_API_URL --yes
uv run prefect config set PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true
uv run python amin/prefect_flow.py
```

## What it does

| Stage | Where it happens | What it produces |
|---|---|---|
| Extract | `requests`, streamed to disk | `amin/data/raw/green_tripdata_2025-0{1,2,3}.parquet` |
| Load | DuckDB `read_parquet` | table `raw_trips` in `amin/data/green_taxi.duckdb`, unchanged |
| Transform | SQL inside DuckDB | table `daily_revenue`, exported to CSV and Parquet |

Last run: 146,486 rows loaded, 90 days in the report, 146,475 trips, 3,395,290.56 in revenue.

## Why ELT and not ETL

The raw rows go into the database first, and the rule that turns them into a report is one SQL
statement. If I get that rule wrong, I change the SQL and rebuild the table in about a second,
without downloading anything again. With ETL the cleaning happens in pandas before anything is
saved, so changing a rule means reading every file again, and the raw rows are not there anymore.

The files are small, so this is not about speed. It is just that the raw rows stay in the
database, so I can build a different report later without starting over.

## Something I noticed in the data

The files are named after a month, but they are not cut exactly. 11 trips have a pickup time
outside the quarter. The transform only keeps `2025-01-01 <= pickup < 2025-04-01`, and
`run_metadata.json` writes down how many rows that dropped, so the number is there to see
instead of quietly changing the totals.

## Answers to the project questions

**1. What steps did you take?**

I read the brief and looked at the solution folder first, then decided to do mine differently:
DuckDB ELT instead of a pandas ETL. Then I wrote the extract step (stream each monthly
file to `data/raw/`, skip files that are already there), the load step (one
`CREATE OR REPLACE TABLE raw_trips AS SELECT * FROM read_parquet(...)`), and the transform step
(one SQL statement grouping by pickup date, exported to CSV and Parquet plus a small metadata
file). I added three tests over a tiny in-memory table so they run without a download, wrapped
the same three functions in a Prefect flow for the bonus, and ran everything end to end.

**2. What challenges did I face?**

- `duckdb` was not in the project dependencies, so I added it to `pyproject.toml`.
- Working out where the cleaning belongs. I kept `raw_trips` exactly as it came and put the date
  filter in the transform, so the raw table always matches the files.
- The stray pickup dates. Without the filter the report gets extra days from 2024 and beyond.
- Prefect still pointed at the local server from the orchestration day, so the flow could not
  reach an API until I unset `PREFECT_API_URL`.

**3. What would I do differently with more time?**

- Load into Postgres instead of a local DuckDB file, so the group could share one database. The
  SQL would stay the same.
- Add the other metrics the group may want (revenue per zone, per payment type) as more SQL
  views on the same raw table.
- Make the month list a command line option instead of a constant, so the pipeline can run for
  any quarter.
- Make the run fail if a day is missing from the output, instead of only counting the rows.
