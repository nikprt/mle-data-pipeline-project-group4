"""
Bonus task: the same ELT pipeline as a Prefect flow.
"""

from pathlib import Path

from prefect import flow, task

from pipeline import connect, extract, load, transform


@task(name="extract", retries=3, retry_delay_seconds=10, log_prints=True)
def extract_task(force: bool = False) -> list[str]:
    return [str(path) for path in extract(force=force)]


@task(name="load", log_prints=True)
def load_task(paths: list[str]) -> int:
    connection = connect()
    try:
        return load(connection, [Path(path) for path in paths])
    finally:
        connection.close()


@task(name="transform", log_prints=True)
def transform_task() -> dict:
    connection = connect()
    try:
        return transform(connection)
    finally:
        connection.close()


@flow(name="Green Taxi ELT", log_prints=True)
def green_taxi_elt(force: bool = False) -> dict:
    paths = extract_task(force=force)
    load_task(paths)
    return transform_task()


if __name__ == "__main__":
    green_taxi_elt()
