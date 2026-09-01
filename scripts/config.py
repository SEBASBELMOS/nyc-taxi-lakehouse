"""Shared configuration and helpers for the Corte 1 pipelines.

Every value is read from an environment variable defined in docker-compose.yml,
so nothing is tied to a specific machine and no host IP is ever hardcoded.
Credentials consumed by dlt itself live in .dlt/secrets.toml.
"""

from __future__ import annotations

import os

# --- Source ----------------------------------------------------------------
SOURCE_URL = os.getenv(
    "SOURCE_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet",
)

# Row count the assignment requires the final ClickHouse table to have.
EXPECTED_ROWS = int(os.getenv("EXPECTED_ROWS", "3475226"))

# --- MinIO (Data Lake) -----------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")

RAW_BUCKET = os.getenv("RAW_BUCKET", "nyc-taxi-raw")
ICEBERG_BUCKET = os.getenv("ICEBERG_BUCKET", "nyc-taxi-iceberg")

# Pipeline 01 writes to s3://<RAW_BUCKET>/<RAW_DATASET>/<RAW_TABLE>/*.parquet
RAW_DATASET = os.getenv("RAW_DATASET", "raw")
RAW_TABLE = os.getenv("RAW_TABLE", "yellow_tripdata")

# --- Nessie (Catalog) / Iceberg (Table Format) -----------------------------
# Nessie exposes an Iceberg REST catalog under /iceberg. Appending a branch
# name (e.g. /iceberg/main) selects that branch; the bare path uses "main".
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/iceberg")
NESSIE_WAREHOUSE = os.getenv("NESSIE_WAREHOUSE", "warehouse")
NESSIE_NAMESPACE = os.getenv("NESSIE_NAMESPACE", "nyc_taxi")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "yellow_tripdata_2025_01")

TABLE_IDENTIFIER = f"{NESSIE_NAMESPACE}.{ICEBERG_TABLE}"

# --- ClickHouse (Data Warehouse) -------------------------------------------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")

# dlt prefixes every table with the dataset name. With the "_" separator set
# in .dlt/config.toml the final table is <CLICKHOUSE_DATASET>_<ICEBERG_TABLE>.
CLICKHOUSE_DATASET = os.getenv("CLICKHOUSE_DATASET", "nyc_taxi")
CLICKHOUSE_TABLE = f"{CLICKHOUSE_DATASET}_{ICEBERG_TABLE}"

# --- Azure -----------------------------------------------------------------
# Folder assigned to the group. The actual destination URL lives in
# .dlt/secrets.toml; this is used to fail early when it is still a placeholder.
GROUP_FOLDER = os.getenv("GROUP_FOLDER", "GRUPO_X")

# --- Tuning ----------------------------------------------------------------
# Rows per PyArrow record batch. The source file has ~3.5M rows; reading it
# whole into memory would spike to several GB inside the container.
BATCH_ROWS = int(os.getenv("BATCH_ROWS", "250000"))


def banner(text: str) -> None:
    """Print a section header. Keeps the terminal readable while recording."""
    line = "=" * 74
    print(f"\n{line}\n  {text}\n{line}", flush=True)


def minio_filesystem():
    """Return an fsspec filesystem pointed at MinIO."""
    import s3fs

    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def iceberg_catalog():
    """Return a PyIceberg catalog backed by Nessie.

    PyIceberg has no "nessie" catalog type. Nessie speaks the Iceberg REST
    protocol, so the catalog type is "rest". Note that the `prefix` property
    must NOT be set: Nessie selects the branch through the URI path instead,
    and `prefix` is documented as not working with PyIceberg.
    """
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "nessie",
        **{
            "uri": NESSIE_URI,
            "warehouse": NESSIE_WAREHOUSE,
            # MinIO is S3-compatible but not AWS: an explicit endpoint and
            # path-style addressing are both required.
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def list_parquet(filesystem, prefix: str) -> list[str]:
    """List Parquet objects under a prefix, flat or nested.

    dlt writes its output flat under <dataset>/<table>/, while Iceberg nests
    data files under data/. Matching both patterns keeps callers simple.
    """
    prefix = prefix.rstrip("/")
    found = set(filesystem.glob(f"{prefix}/*.parquet"))
    found |= set(filesystem.glob(f"{prefix}/**/*.parquet"))
    return sorted(found)


def clickhouse_client():
    """Return a ClickHouse client over the HTTP interface."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
