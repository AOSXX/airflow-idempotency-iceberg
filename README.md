# Airflow · Idempotency · Iceberg

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white"/>
  <img src="https://img.shields.io/badge/Apache%20Iceberg-2D6EBA?style=for-the-badge&logo=apache&logoColor=white"/>
  <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white"/>
  <img src="https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white"/>
  <img src="https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
</p>

<p align="center">
  A production-grade, fully local data lakehouse demo covering one of the most critical : and most overlooked : properties of ETL pipelines: <strong>idempotency</strong>.
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#the-three-dags">The three DAGs</a> ·
  <a href="#stack">Stack</a> ·
  <a href="#key-concepts">Key concepts</a>
</p>

---

## What this project demonstrates

| Skill | How it's shown |
|-------|---------------|
| ETL pipeline design | Three DAGs that progressively fix a real production bug |
| Idempotency patterns | Blind INSERT → DELETE+INSERT → Iceberg partition overwrite |
| Apache Iceberg | Table creation, atomic partition overwrite, snapshot history, time-travel metadata |
| Open table format | Parquet files + Iceberg metadata served from a local S3-compatible store |
| Airflow best practices | `logical_date` as partition key, `@task` decorator, XCom data flow |
| Data lakehouse (local) | MinIO + Iceberg REST catalog + DuckDB as a zero-cloud lakehouse |
| Docker Compose | Multi-service orchestration: Airflow, Postgres, MinIO, Iceberg catalog |

---

## The three DAGs

### `01` : Non-idempotent ETL ⚠️

A plain `INSERT` into a target table. Trigger it twice on the same date and the row count doubles. This is the bug that corrupts data silently in production.

```sql
-- Run after two triggers : watch it double
SELECT order_date, COUNT(*) FROM orders_raw GROUP BY 1 ORDER BY 1;
```

### `02` : Idempotent SQL ETL ✅

Classic `DELETE + INSERT` partitioned by `logical_date`. Works on any relational database. Re-run as many times as you like : the result is always the same.

```sql
-- Always exactly 1 row per date, no matter how many reruns
SELECT * FROM orders_daily_summary ORDER BY summary_date;
```

### `03` : Idempotent Iceberg ETL 🧊

Writes to an Apache Iceberg table on MinIO using an **atomic partition overwrite**. No window between delete and insert : readers never see a partial state. Results are audited live with DuckDB.

```python
table.overwrite(df=arrow_table, overwrite_filter=EqualTo("order_date", days_since_epoch))
```

---

## Stack

> 100% local : no cloud account, no paid services.

```
┌──────────────────────────────────────────────────────────────┐
│                       Docker Compose                         │
│                                                              │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────┐   │
│  │ PostgreSQL  │    │   Airflow    │    │     MinIO      │   │
│  │  source DB  │◄───│  scheduler   │───►│  (local S3)    │   │
│  │  + targets  │    │  webserver   │    └────────┬───────┘   │
│  └─────────────┘    └──────────────┘            │            │
│                                         ┌───────▼────────┐   │
│                                         │ Iceberg REST   │   │
│                                         │    Catalog     │   │
│                                         └────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

| Service | Image | Port |
|---------|-------|------|
| PostgreSQL | `postgres:15-alpine` | 5432 |
| Airflow | custom `apache/airflow:2.9.3` | **8080** |
| MinIO | `minio/minio:latest` | 9000 / **9001** |
| Iceberg REST catalog | `tabulario/iceberg-rest:0.10.0` | 8181 |

### Why not Snowflake locally?

Snowflake has no open-source local image. However, **Apache Iceberg is the open table format that Snowflake's "Iceberg Tables" feature is built on**. Everything here maps directly to Snowflake Open Catalog (Polaris), AWS Glue, Databricks, and Google BigLake : learn once, use everywhere.

---

## Key concepts

### Idempotency

> Running the same operation multiple times produces the same result as running it once.

The golden rule in Airflow: use `{{ ds }}` (the logical date) as your partition key : never `NOW()`. This makes every DAG run deterministic and safe to re-run.

### Apache Iceberg internals

Iceberg stores data as regular Parquet files, with a metadata layer that provides ACID guarantees:

```
metadata.json          ← table history + current snapshot pointer
    └── snap-....avro  ← manifest list (one per snapshot)
            └── -m0.avro  ← manifest file (one row per Parquet file)
                    └── part-0001.parquet  ← actual data
```

A **partition overwrite** is atomic: new Parquet files are written first, then a single metadata commit swaps the snapshot. Old files remain on disk for time-travel until explicitly expired : they are invisible to current readers.

---

### Iceberg ↔ Snowflake

💬 *"If you use Snowflake today, know that Iceberg Tables in Snowflake write the same Parquet + metadata format. Everything you learned here translates directly : including the partition overwrite semantics."*

### Common questions

**Q: Can I use MERGE/UPSERT instead of DELETE+INSERT?**
Yes! `INSERT ... ON CONFLICT DO UPDATE` (PostgreSQL) or `MERGE` (BigQuery, Snowflake) is equivalent but does it in one statement instead of two. The choice depends on your database.

**Q: Does Iceberg work with Spark?**
Yes : Spark is the most common Iceberg engine in production. This demo uses PyIceberg (pure Python) to keep the stack lightweight. In production, replace the PyIceberg write with a Spark `DataFrame.writeTo(...).overwritePartitions()`.

**Q: What about Delta Lake vs Iceberg?**
Both are open table formats with ACID guarantees. Iceberg is vendor-neutral (Apache project), Delta is originally from Databricks but now open. Iceberg has stronger partition evolution support; Delta has tighter Spark integration.

---

## Quick start

```bash
# 1. Clone
git clone <repo-url> && cd airflow-recap

# 2. Build custom Airflow image (PyIceberg, DuckDB, psycopg2)
docker compose build

# 3. Initialise (run once)
docker compose up airflow-init
# → wait for "Airflow initialised."

# 4. Start everything
docker compose up -d

# 5. Open UIs
open http://localhost:8080   # Airflow  : admin / admin
open http://localhost:9001   # MinIO    : minio / minio123
```

### Reset everything

```bash
docker compose down -v   # removes containers + all volumes
```

---

## Project layout

```
airflow-recap/
├── Dockerfile                        # Airflow + PyIceberg + DuckDB
├── docker-compose.yml
├── dags/
│   ├── 01_non_idempotent_etl.py      # ⚠️  Bad pattern  : blind INSERT
│   ├── 02_idempotent_sql_etl.py      # ✅  Good pattern : DELETE + INSERT
│   └── 03_idempotent_iceberg_etl.py  # 🧊  Best pattern : Iceberg overwrite
└── scripts/
    └── init_db.sql                   # Seeds 200 orders across 10 days
```

---

## License

See [LICENSE](LICENSE).
