"""
DAG 01 – NON-IDEMPOTENT ETL  (the bad pattern)
===============================================
Extracts orders for the execution date and does a blind INSERT into
orders_raw.  Running the same logical date twice → duplicate rows.

Run it twice with the same date and query:
    SELECT COUNT(*) FROM orders_raw WHERE order_date = '<date>';
You will see the count double on every run.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook


DB_CONN_ID = "source_postgres"


def _get_conn():
    conn_info = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port or 5432,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )


@dag(
    dag_id="01_non_idempotent_etl",
    description="⚠️  BAD PATTERN – blind INSERT, creates duplicates on re-run",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 0},
    tags=["idempotency", "demo", "bad-pattern"],
)
def non_idempotent_etl():

    @task
    def extract(logical_date=None) -> list[dict]:
        """Read orders for the execution date from the source table."""
        exec_date = logical_date.date() if logical_date else datetime.utcnow().date()
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, customer_id, product, amount, order_date, status
                FROM   orders
                WHERE  order_date = %s
                """,
                (exec_date,),
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        print(f"Extracted {len(rows)} rows for {exec_date}")
        return rows

    @task
    def load(rows: list[dict]) -> None:
        """
        BAD: plain INSERT with no duplicate check.
        Every re-run of the same logical date appends another copy.
        """
        if not rows:
            print("Nothing to load.")
            return

        with _get_conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO orders_raw
                    (order_id, customer_id, product, amount, order_date, status)
                VALUES
                    (%(order_id)s, %(customer_id)s, %(product)s,
                     %(amount)s,   %(order_date)s,  %(status)s)
                """,
                rows,
            )
            conn.commit()
        print(f"Inserted {len(rows)} rows – run again and the count will DOUBLE!")

    @task
    def audit(logical_date=None) -> None:
        """Show how many rows exist in the target for this date."""
        exec_date = logical_date.date() if logical_date else datetime.utcnow().date()
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM orders_raw WHERE order_date = %s", (exec_date,)
            )
            total = cur.fetchone()[0]
        print(f"[AUDIT] orders_raw has {total} rows for {exec_date}. Re-run to see duplication!")

    rows = extract()
    loaded = load(rows)
    loaded >> audit()


non_idempotent_etl()
