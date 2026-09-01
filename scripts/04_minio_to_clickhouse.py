"""04 - MinIO (Iceberg data files) -> ClickHouse, loaded with dlt.

Requirement RF-04: insert the Parquet backing the Iceberg table into
ClickHouse and prove the table holds exactly 3,475,226 rows.

Run:  python scripts/04_minio_to_clickhouse.py
"""

from __future__ import annotations

import sys

import dlt
from dlt.sources.filesystem import readers

import config as cfg

PIPELINE_NAME = "minio_to_clickhouse"


def iceberg_data_prefix() -> str:
    """Ask Nessie where the Iceberg table stores its data files."""
    catalog = cfg.iceberg_catalog()
    if not catalog.table_exists(cfg.TABLE_IDENTIFIER):
        raise SystemExit(
            f"Table {cfg.TABLE_IDENTIFIER} does not exist in Nessie.\n"
            "Run 02_parquet_to_iceberg.py first."
        )
    location = catalog.load_table(cfg.TABLE_IDENTIFIER).location()
    return f"{location.rstrip('/')}/data/"


def main() -> int:
    cfg.banner("04 | MinIO (Iceberg) -> ClickHouse")

    source_prefix = iceberg_data_prefix()
    print(f"Source     : {source_prefix}")
    print(f"Destination: clickhouse://{cfg.CLICKHOUSE_HOST}/{cfg.CLICKHOUSE_DATABASE}")
    print(f"Table      : {cfg.CLICKHOUSE_TABLE}")

    reader = readers(bucket_url=source_prefix, file_glob="**/*.parquet").read_parquet()
    reader = reader.with_name(cfg.ICEBERG_TABLE)

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="clickhouse",
        dataset_name=cfg.CLICKHOUSE_DATASET,
    )

    # replace, not append: guarantees the count is exact on every re-run.
    load_info = pipeline.run(reader, write_disposition="replace")
    print(load_info)

    cfg.banner("Verification | ClickHouse")
    client = cfg.clickhouse_client()

    print(f"SHOW TABLES FROM {cfg.CLICKHOUSE_DATABASE};\n")
    tables = [row[0] for row in client.query(
        f"SHOW TABLES FROM {cfg.CLICKHOUSE_DATABASE}"
    ).result_rows]
    for name in tables:
        marker = "  <- data" if name == cfg.CLICKHOUSE_TABLE else ""
        marker = marker or ("  <- dlt metadata" if "_dlt_" in name else "")
        print(f"  {name}{marker}")

    if cfg.CLICKHOUSE_TABLE not in tables:
        print(f"\nFAIL: table {cfg.CLICKHOUSE_TABLE} was not created.")
        return 1

    count = client.query(
        f"SELECT count(*) FROM {cfg.CLICKHOUSE_DATABASE}.{cfg.CLICKHOUSE_TABLE}"
    ).result_rows[0][0]

    print(f"\nSELECT count(*) FROM {cfg.CLICKHOUSE_TABLE};")
    print(f"  -> {count:,}")
    print(f"  expected: {cfg.EXPECTED_ROWS:,}")

    if count != cfg.EXPECTED_ROWS:
        print("\nFAIL: row count does not match the expected value.")
        return 1

    print("\nOK - ClickHouse holds exactly the expected number of rows.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
