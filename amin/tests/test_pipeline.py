"""
Tests for the SQL transform, on a tiny table in memory.
"""

import duckdb
import pytest

from pipeline import DAILY_REVENUE_SQL

ROWS = """
CREATE OR REPLACE TABLE raw_trips AS
SELECT * FROM (VALUES
    (TIMESTAMP '2025-01-01 08:00:00', 10.5),
    (TIMESTAMP '2025-01-01 09:30:00', 4.5),
    (TIMESTAMP '2025-01-02 14:00:00', 15.0),
    (TIMESTAMP '2024-12-31 23:00:00', 99.0),
    (TIMESTAMP '2025-04-01 00:30:00', 77.0)
) AS t(lpep_pickup_datetime, total_amount)
"""


@pytest.fixture
def connection():
    con = duckdb.connect()
    con.execute(ROWS)
    yield con
    con.close()


def test_daily_revenue_groups_by_pickup_date(connection):
    connection.execute(DAILY_REVENUE_SQL)
    rows = connection.execute("SELECT service_date::VARCHAR, trips, revenue FROM daily_revenue").fetchall()
    assert rows == [("2025-01-01", 2, 15.0), ("2025-01-02", 1, 15.0)]


def test_trips_outside_the_quarter_are_dropped(connection):
    """
    The files contain a few trips dated before and after the quarter.
    """
    connection.execute(DAILY_REVENUE_SQL)
    total_trips = connection.execute("SELECT SUM(trips) FROM daily_revenue").fetchone()[0]
    assert total_trips == 3


def test_average_is_revenue_per_trip(connection):
    connection.execute(DAILY_REVENUE_SQL)
    avg = connection.execute("SELECT avg_revenue_per_trip FROM daily_revenue WHERE service_date = DATE '2025-01-01'").fetchone()[0]
    assert avg == 7.5
