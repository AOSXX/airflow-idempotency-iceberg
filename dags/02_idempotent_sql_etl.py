"""
DAG 02 – IDEMPOTENT ETL via DELETE + INSERT  (SQL pattern)
===========================================================
Classic approach that works with ANY relational database.

Strategy
--------
1. DELETE all rows for the execution date partition.
2. INSERT fresh rows for that partition.

Running the same logical date N times always produces the same
final state → idempotent by construction.

Key Airflow concept: use {{ ds }} (the logical/execution date as
YYYY-MM-DD) as the partition key so each DAG run owns exactly one
date partition and never touches another run's data.
"""

from __future__ import annotations

from datetime import datetime

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
    dag_id="02_idempotent_sql_etl",
    description="✅  GOOD PATTERN – DELETE + INSERT guarantees idempotency",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 0},
    tags=["idempotency", "demo", "good-pattern"],
)
def idempotent_sql_etl():

    @task
    def extract(logical_date=None) -> list[dict]:
        """Read orders for the execution date from source."""
        exec_date = logical_date.date() if logical_date else datetime.utcnow().date()
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT   order_date,
                         COUNT(*)                         AS total_orders,
                         SUM(amount)                      AS total_revenue,
                         SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END)
                                                          AS completed_orders,
                         ROUND(AVG(amount), 2)            AS avg_order_value
                FROM     orders
                WHERE    order_date = %s
                GROUP BY order_date
                """,
                (exec_date,),
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        print(f"Aggregated {len(rows)} summary row(s) for {exec_date}")
        return rows

    @task
    def delete_partition(logical_date=None) -> str:
        """
        STEP 1 – Wipe the target partition for this logical date.
        This makes the subsequent INSERT safe to re-run.
        """
        exec_date = logical_date.date() if logical_date else datetime.utcnow().date()
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM orders_daily_summary WHERE summary_date = %s",
                (exec_date,),
            )
            deleted = cur.rowcount
            conn.commit()
        print(f"Deleted {deleted} existing row(s) for partition {exec_date}")
        return str(exec_date)

    @task
    def insert_partition(rows: list[dict], partition_date: str) -> None:
        """
        STEP 2 – Insert fresh aggregates for this partition.
        Safe because STEP 1 already cleared the slate.
        """
        if not rows:
            print(f"No source data for {partition_date}, skipping insert.")
            return

        with _get_conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO orders_daily_summary
                    (summary_date, total_orders, total_revenue,
                     completed_orders, avg_order_value, refreshed_at)
                VALUES
                    (%(order_date)s, %(total_orders)s, %(total_revenue)s,
                     %(completed_orders)s, %(avg_order_value)s, NOW())
                """,
                rows,
            )
            conn.commit()
        print(f"Inserted {len(rows)} row(s) for partition {partition_date}")

    @task
    def audit(logical_date=None) -> None:
        """Prove idempotency: count stays at 1 no matter how many re-runs."""
        exec_date = logical_date.date() if logical_date else datetime.utcnow().date()
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT total_orders, total_revenue, refreshed_at
                FROM   orders_daily_summary
                WHERE  summary_date = %s
                """,
                (exec_date,),
            )
            row = cur.fetchone()
        if row:
            print(
                f"[AUDIT] {exec_date}: "
                f"orders={row[0]}, revenue={row[1]}, refreshed={row[2]}"
            )
            print("Re-run this DAG → same numbers, same 1 row. That's idempotency!")
        else:
            print(f"[AUDIT] No data for {exec_date}")

    rows = extract()
    partition_date = delete_partition()
    inserted = insert_partition(rows, partition_date)
    inserted >> audit()


idempotent_sql_etl()
