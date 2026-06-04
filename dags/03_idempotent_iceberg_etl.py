"""
DAG 03 – IDEMPOTENT ETL via Apache Iceberg Partition Overwrite  (modern pattern)
=================================================================================
Apache Iceberg is an open table format designed for large analytic datasets.
It runs on any S3-compatible storage (here: MinIO) and exposes a REST catalog.

Why Iceberg for idempotency?
----------------------------
• Partition overwrite is atomic:  the old partition files are replaced in a
  single metadata commit.  Readers never see a partial state.
• ACID guarantees:  no partial writes, no "delete then fail before insert".
• Schema evolution, time-travel, and partition evolution come for free.
• The same table format powers Snowflake Open Catalog, AWS Glue, Databricks
  Delta (Parquet-compatible), and Google BigLake → learn once, use everywhere.

Stack in this demo
------------------
  PostgreSQL  →  Airflow DAG  →  PyIceberg  →  MinIO (S3)
                                              ↑
                                    Iceberg REST Catalog

Running the same logical date twice results in exactly the same Parquet files
on MinIO, because the partition overwrite replaces rather than appends.
"""

from __future__ import annotations

import os
from datetime import datetime

import psycopg2
import pyarrow as pa
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform


DB_CONN_ID = "source_postgres"
NAMESPACE = "demo"
TABLE_NAME = "orders"
FULL_TABLE = f"{NAMESPACE}.{TABLE_NAME}"


def _get_pg_conn():
    conn_info = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn_info.host,
        port=conn_info.port or 5432,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )


def _get_iceberg_catalog() -> RestCatalog:
    """Return a configured PyIceberg REST catalog pointing at MinIO."""
    return RestCatalog(
        name="rest",
        uri=os.environ["ICEBERG_REST_URI"],          # http://iceberg-rest:8181
        **{
            "s3.endpoint": os.environ["MINIO_ENDPOINT"],  # http://minio:9000
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.path-style-access": "true",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )


# Iceberg schema for the orders table
ORDERS_SCHEMA = Schema(
    NestedField(1,  "order_id",    LongType(),      required=False),
    NestedField(2,  "customer_id", IntegerType(),   required=False),
    NestedField(3,  "product",     StringType(),    required=False),
    NestedField(4,  "amount",      DoubleType(),    required=False),
    NestedField(5,  "order_date",  DateType(),      required=False),
    NestedField(6,  "status",      StringType(),    required=False),
    NestedField(7,  "loaded_at",   TimestampType(), required=False),
)

# Partition by day on order_date → each DAG run owns one partition
ORDERS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=5, field_id=1000, transform=DayTransform(), name="order_date_day")
)


@dag(
    dag_id="03_idempotent_iceberg_etl",
    description="🧊  BEST PATTERN – Apache Iceberg partition overwrite (atomic + idempotent)",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 0},
    tags=["idempotency", "demo", "iceberg", "best-pattern"],
)
def idempotent_iceberg_etl():

    @task
    def ensure_table() -> None:
        """Create the Iceberg namespace and table if they don't exist yet."""
        from pyiceberg.exceptions import ServerError

        catalog = _get_iceberg_catalog()
        try:
            catalog.create_namespace(NAMESPACE)
            print(f"Created namespace '{NAMESPACE}'")
        except NamespaceAlreadyExistsError:
            pass

        try:
            catalog.load_table(FULL_TABLE)
            print(f"Table '{FULL_TABLE}' already exists.")
        except NoSuchTableError:
            catalog.create_table(
                identifier=FULL_TABLE,
                schema=ORDERS_SCHEMA,
                partition_spec=ORDERS_PARTITION_SPEC,
            )
            print(f"Created Iceberg table '{FULL_TABLE}'")
        except ServerError:
            # Catalog has the table registered but the metadata files were deleted
            # (e.g. MinIO bucket was wiped manually). Drop the stale entry and recreate.
            print(f"Stale catalog entry for '{FULL_TABLE}', dropping and recreating.")
            catalog.drop_table(FULL_TABLE, purge_requested=False)
            catalog.create_table(
                identifier=FULL_TABLE,
                schema=ORDERS_SCHEMA,
                partition_spec=ORDERS_PARTITION_SPEC,
            )
            print(f"Recreated Iceberg table '{FULL_TABLE}'")

    @task
    def extract(logical_date=None) -> list[dict]:
        """Read raw orders for the execution date from PostgreSQL."""
        exec_date = logical_date.date() if logical_date else datetime.utcnow().date()
        with _get_pg_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, customer_id, product,
                       amount::FLOAT, order_date, status
                FROM   orders
                WHERE  order_date = %s
                """,
                (exec_date,),
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        print(f"Extracted {len(rows)} orders for {exec_date}")
        return rows

    @task
    def overwrite_partition(rows: list[dict], logical_date=None) -> None:
        """
        CORE CONCEPT – Iceberg partition overwrite.

        Instead of DELETE + INSERT (two operations that can leave a gap),
        Iceberg writes a new snapshot that atomically replaces the partition.

        write_to_table() with overwrite=True and a filter on the partition
        guarantees that re-running the same logical date produces identical
        Parquet files on MinIO.  The old files are marked as deleted in the
        Iceberg metadata but remain on disk until expiry (great for auditing).
        """
        from datetime import date as date_type
        import pyarrow as pa
        from pyiceberg.expressions import EqualTo

        exec_date = logical_date.date() if logical_date else datetime.utcnow().date()

        if not rows:
            print(f"No data for {exec_date}, nothing to write.")
            return

        # Build a PyArrow table from the extracted rows
        arrow_table = pa.table(
            {
                "order_id":    pa.array([r["order_id"]    for r in rows], type=pa.int64()),
                "customer_id": pa.array([r["customer_id"] for r in rows], type=pa.int32()),
                "product":     pa.array([r["product"]     for r in rows], type=pa.string()),
                "amount":      pa.array([r["amount"]      for r in rows], type=pa.float64()),
                "order_date":  pa.array([r["order_date"]  for r in rows], type=pa.date32()),
                "status":      pa.array([r["status"]      for r in rows], type=pa.string()),
                "loaded_at":   pa.array([datetime.utcnow()] * len(rows), type=pa.timestamp("us")),
            }
        )

        catalog = _get_iceberg_catalog()
        table = catalog.load_table(FULL_TABLE)

        # PyIceberg's EqualTo requires DateType values as days since Unix epoch,
        # not a datetime.date object.
        from datetime import date as date_cls
        days_since_epoch = (exec_date - date_cls(1970, 1, 1)).days

        table.overwrite(
            df=arrow_table,
            overwrite_filter=EqualTo("order_date", days_since_epoch),
        )
        print(
            f"Overwrote partition order_date={exec_date} "
            f"with {len(rows)} rows. Re-run → same result!"
        )

    @task
    def audit_with_duckdb(logical_date=None) -> None:
        """
        Query the Iceberg table with DuckDB to confirm row counts.
        DuckDB can read Iceberg tables directly via PyIceberg scan → Arrow.
        """
        import duckdb

        exec_date = logical_date.date() if logical_date else datetime.utcnow().date()

        catalog = _get_iceberg_catalog()
        table = catalog.load_table(FULL_TABLE)

        # Scan only the relevant partition (predicate pushdown)
        from datetime import date as date_cls
        from pyiceberg.expressions import EqualTo
        days_since_epoch = (exec_date - date_cls(1970, 1, 1)).days
        arrow = table.scan(
            row_filter=EqualTo("order_date", days_since_epoch)
        ).to_arrow()

        result = duckdb.query(
            "SELECT order_date, COUNT(*) AS n, ROUND(SUM(amount),2) AS revenue "
            "FROM arrow GROUP BY order_date"
        ).fetchall()

        print("[AUDIT via DuckDB]")
        for row in result:
            print(f"  date={row[0]}, orders={row[1]}, revenue={row[2]}")

        # Also show table snapshots – each run adds exactly 1 new snapshot
        history = table.history()
        print(f"\nIceberg snapshot count: {len(history)} (one per run)")

    setup = ensure_table()
    rows = extract()
    written = overwrite_partition(rows)
    audit = audit_with_duckdb()

    setup >> rows >> written >> audit


idempotent_iceberg_etl()
