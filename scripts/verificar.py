"""Evidence report - walks every checkpoint the assignment is graded on.

This is the script to run on camera at the end of the video: it proves, in one
pass, that each component of the stack is up and holds what it should.

Run:  python scripts/verificar.py
"""

from __future__ import annotations

import sys
from urllib.parse import urlparse

import config as cfg

results: list[tuple[str, bool, str]] = []


def check(label: str, ok: bool, detail: str = "") -> bool:
    results.append((label, ok, detail))
    print(f"  [{'OK ' if ok else 'FAIL'}] {label}" + (f" - {detail}" if detail else ""))
    return ok


def check_minio() -> None:
    cfg.banner("MinIO | Data Lake")
    try:
        filesystem = cfg.minio_filesystem()
        buckets = [b.strip("/") for b in filesystem.ls("/")]
        print(f"Buckets: {buckets}\n")

        for bucket in (cfg.RAW_BUCKET, cfg.ICEBERG_BUCKET):
            check(f"bucket '{bucket}' exists", bucket in buckets)

        raw = cfg.list_parquet(filesystem, cfg.RAW_BUCKET)
        check("RAW bucket has Parquet files", bool(raw), f"{len(raw)} file(s)")
        for key in raw[:5]:
            print(f"        {key}")

        data = cfg.list_parquet(filesystem, cfg.ICEBERG_BUCKET)
        meta = filesystem.glob(f"{cfg.ICEBERG_BUCKET}/**/metadata/*")
        check("Iceberg bucket has data files", bool(data), f"{len(data)} file(s)")
        check("Iceberg bucket has metadata files", bool(meta), f"{len(meta)} file(s)")
    except Exception as error:  # noqa: BLE001 - report, do not crash the run
        check("MinIO reachable", False, str(error))


def check_nessie() -> None:
    cfg.banner("Nessie | Catalog")
    try:
        catalog = cfg.iceberg_catalog()
        namespaces = [".".join(n) for n in catalog.list_namespaces()]
        print(f"Namespaces: {namespaces}\n")
        check(f"namespace '{cfg.NESSIE_NAMESPACE}' exists", cfg.NESSIE_NAMESPACE in namespaces)

        exists = catalog.table_exists(cfg.TABLE_IDENTIFIER)
        check(f"table '{cfg.TABLE_IDENTIFIER}' registered", exists)
        if not exists:
            return

        table = catalog.load_table(cfg.TABLE_IDENTIFIER)
        print(f"\n        location : {table.location()}")
        print(f"        snapshots: {len(table.metadata.snapshots)}")
        print(f"        columns  : {len(table.schema().fields)}")

        rows = table.scan().to_arrow().num_rows
        check(
            "Iceberg row count matches",
            rows == cfg.EXPECTED_ROWS,
            f"{rows:,} (expected {cfg.EXPECTED_ROWS:,})",
        )
    except Exception as error:  # noqa: BLE001
        check("Nessie reachable", False, str(error))


def check_clickhouse() -> None:
    cfg.banner("ClickHouse | Data Warehouse")
    try:
        client = cfg.clickhouse_client()
        tables = [r[0] for r in client.query(
            f"SHOW TABLES FROM {cfg.CLICKHOUSE_DATABASE}"
        ).result_rows]

        print(f"SHOW TABLES FROM {cfg.CLICKHOUSE_DATABASE};")
        for name in tables:
            print(f"        {name}")
        print()

        check(f"table '{cfg.CLICKHOUSE_TABLE}' exists", cfg.CLICKHOUSE_TABLE in tables)

        metadata_tables = [t for t in tables if "_dlt_" in t]
        check("dlt metadata tables present", bool(metadata_tables), ", ".join(metadata_tables))

        if cfg.CLICKHOUSE_TABLE in tables:
            count = client.query(
                f"SELECT count(*) FROM {cfg.CLICKHOUSE_DATABASE}.{cfg.CLICKHOUSE_TABLE}"
            ).result_rows[0][0]
            check(
                "ClickHouse row count matches",
                count == cfg.EXPECTED_ROWS,
                f"{count:,} (expected {cfg.EXPECTED_ROWS:,})",
            )

        for name in metadata_tables:
            rows = client.query(
                f"SELECT count(*) FROM {cfg.CLICKHOUSE_DATABASE}.{name}"
            ).result_rows[0][0]
            print(f"        {name}: {rows:,} row(s)")
    except Exception as error:  # noqa: BLE001
        check("ClickHouse reachable", False, str(error))


def check_azure() -> None:
    cfg.banner("Azure ADLS | remote destination")
    if cfg.GROUP_FOLDER == "GRUPO_X":
        print("  SKIPPED - GROUP_FOLDER is still the placeholder 'GRUPO_X'.")
        return
    try:
        import adlfs
        import dlt

        prefix = "minio_to_azure.destination.filesystem"
        bucket_url = dlt.secrets[f"{prefix}.bucket_url"]
        filesystem = adlfs.AzureBlobFileSystem(
            account_name=dlt.secrets[f"{prefix}.credentials.azure_storage_account_name"],
            account_key=dlt.secrets[f"{prefix}.credentials.azure_storage_account_key"],
        )

        parsed = urlparse(bucket_url)
        container = parsed.netloc.split("@", 1)[0]
        entries = filesystem.find(f"{container}/{parsed.path.lstrip('/')}")

        print(f"Destination: {bucket_url}\n")
        for entry in entries[:10]:
            print(f"        {entry}")
        check("files present in Azure", bool(entries), f"{len(entries)} file(s)")
    except Exception as error:  # noqa: BLE001
        check("Azure reachable", False, str(error))


def main() -> int:
    cfg.banner("PROYECTO CORTE 1 | evidence report")

    check_minio()
    check_nessie()
    check_clickhouse()
    check_azure()

    cfg.banner("Summary")
    failed = [label for label, ok, _ in results if not ok]
    for label, ok, detail in results:
        print(f"  [{'OK ' if ok else 'FAIL'}] {label}" + (f" - {detail}" if detail else ""))

    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed.")
    if failed:
        print("Failed: " + ", ".join(failed))
        return 1

    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
