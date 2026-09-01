"""02 - MinIO RAW Parquet -> Apache Iceberg table in the Nessie catalog.

Requirement RF-02: read the RAW Parquet from MinIO with PyArrow and write it
to a second MinIO bucket as an Apache Iceberg table, registered in Nessie.

Roles, kept explicit because the assignment grades them separately:
    Parquet  = file format   (physical files)
    Iceberg  = table format  (organizes those files into a table)
    Nessie   = catalog       (knows where the table lives)
    MinIO    = storage       (holds both data and metadata)

Run:  python scripts/02_parquet_to_iceberg.py
"""

from __future__ import annotations

import sys

import pyarrow as pa
import pyarrow.parquet as pq

import config as cfg

# dlt stamps every row it loads with these. They are load bookkeeping, not
# source data, so the Iceberg table stays a faithful copy of the original.
DLT_INTERNAL_COLUMNS = ("_dlt_load_id", "_dlt_id")


def strip_dlt_columns(table: pa.Table) -> pa.Table:
    present = [name for name in DLT_INTERNAL_COLUMNS if name in table.column_names]
    return table.drop_columns(present) if present else table


def main() -> int:
    cfg.banner("02 | MinIO RAW Parquet -> Iceberg (Nessie catalog)")
    print(f"Source   : s3://{cfg.RAW_BUCKET}/{cfg.RAW_DATASET}/{cfg.RAW_TABLE}/")
    print(f"Catalog  : {cfg.NESSIE_URI}  (warehouse: {cfg.NESSIE_WAREHOUSE})")
    print(f"Table    : {cfg.TABLE_IDENTIFIER}")

    filesystem = cfg.minio_filesystem()
    prefix = f"{cfg.RAW_BUCKET}/{cfg.RAW_DATASET}/{cfg.RAW_TABLE}"
    source_files = cfg.list_parquet(filesystem, prefix)

    if not source_files:
        print(f"FAIL: no RAW Parquet found under s3://{prefix}/")
        print("Run 01_http_to_minio.py first.")
        return 1

    print(f"\nRAW files found: {len(source_files)}")
    for key in source_files:
        print(f"  {key}")

    # The schema is taken from the first file, minus dlt's bookkeeping columns.
    with filesystem.open(source_files[0], "rb") as handle:
        arrow_schema = strip_dlt_columns(
            pq.ParquetFile(handle).schema_arrow.empty_table()
        ).schema

    cfg.banner("Nessie | namespace and table")
    catalog = cfg.iceberg_catalog()

    catalog.create_namespace_if_not_exists((cfg.NESSIE_NAMESPACE,))
    print(f"Namespace ready: {cfg.NESSIE_NAMESPACE}")

    # Recreating the table makes the script idempotent and guarantees the
    # final row count is exact rather than a multiple of the number of runs.
    if catalog.table_exists(cfg.TABLE_IDENTIFIER):
        print(f"Dropping existing table {cfg.TABLE_IDENTIFIER}")
        catalog.drop_table(cfg.TABLE_IDENTIFIER)

    iceberg_table = catalog.create_table(cfg.TABLE_IDENTIFIER, schema=arrow_schema)
    print(f"Table created at: {iceberg_table.location()}")

    cfg.banner("Writing data")
    written = 0
    for key in source_files:
        with filesystem.open(key, "rb") as handle:
            parquet_file = pq.ParquetFile(handle)
            for batch in parquet_file.iter_batches(batch_size=cfg.BATCH_ROWS):
                chunk = strip_dlt_columns(pa.Table.from_batches([batch]))
                iceberg_table.append(chunk)
                written += chunk.num_rows
                print(f"  appended {written:,} rows", flush=True)

    cfg.banner("Verification")
    iceberg_table.refresh()
    scanned = iceberg_table.scan().to_arrow().num_rows
    print(f"Rows in Iceberg table : {scanned:,}")
    print(f"Snapshots             : {len(iceberg_table.metadata.snapshots)}")
    print(f"Namespaces in Nessie  : {catalog.list_namespaces()}")
    print(f"Tables in namespace   : {catalog.list_tables(cfg.NESSIE_NAMESPACE)}")

    data_files = cfg.list_parquet(filesystem, cfg.ICEBERG_BUCKET)
    metadata_files = sorted(filesystem.glob(f"{cfg.ICEBERG_BUCKET}/**/metadata/*"))
    print(f"\nData files in s3://{cfg.ICEBERG_BUCKET}     : {len(data_files)}")
    print(f"Metadata files in s3://{cfg.ICEBERG_BUCKET} : {len(metadata_files)}")

    if scanned != cfg.EXPECTED_ROWS:
        print(f"\nWARNING: expected {cfg.EXPECTED_ROWS:,} rows, found {scanned:,}")
        return 1

    print("\nOK - Iceberg table matches the expected row count.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
