"""
Serves the taxi ETL flow as a Prefect deployment so it shows up in the
Prefect UI/API (http://127.0.0.1:4200) and can be triggered on demand
or on a schedule, instead of only being runnable as `python etl_pipeline.py`.

Prerequisite: a Prefect server must be running (in a separate terminal):
    prefect server start

Usage:
    python etl_deploy.py
Leave this running — it's the process that actually executes flow runs
when they're triggered from the UI, the CLI, or the schedule below.
"""

from etl_pipeline import taxi_etl_flow

if __name__ == "__main__":
    taxi_etl_flow.serve(
        name="green-taxi-etl-deployment",
        tags=["taxi", "etl"],
        parameters={"color_target": "green", "months_target": [1, 2, 3]},
    )
