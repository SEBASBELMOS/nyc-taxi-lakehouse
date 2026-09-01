"""03 - MinIO (Iceberg data files) -> Azure ADLS, loaded with dlt.

Requirement RF-03: move the Parquet backing the Iceberg table from MinIO to
the group's folder in Azure Data Lake Storage, using dlt.

The source prefix is not hardcoded: it is asked of the Nessie catalog, so the
script keeps working if the warehouse layout or table name changes.

Run:  python scripts/03_minio_to_azure.py
"""

from __future__ import annotations

import sys
from urllib.parse import urlparse

import dlt
from dlt.sources.filesystem import readers

import config as cfg

PIPELINE_NAME = "minio_to_azure"


def secret(path: str, default=None):
    """Read a value from .dlt/secrets.toml without duplicating it in code."""
    try:
        return dlt.secrets[path]
    except Exception:  # noqa: BLE001 - dlt raises its own lookup error types
        return default


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


def verify_in_azure(bucket_url: str) -> int:
    """List what actually landed in ADLS.

    This is the 'investigate how to verify the file in Azure' deliverable:
    adlfs speaks the same abfss:// scheme dlt writes to, using the same
    credentials, so verification does not depend on the Azure portal.
    """
    import adlfs

    account_name = secret(f"{PIPELINE_NAME}.destination.filesystem.credentials.azure_storage_account_name")
    account_key = secret(f"{PIPELINE_NAME}.destination.filesystem.credentials.azure_storage_account_key")

    parsed = urlparse(bucket_url)          # abfss://container@account.dfs.../path
    container = parsed.netloc.split("@", 1)[0]
    path = parsed.path.lstrip("/")

    filesystem = adlfs.AzureBlobFileSystem(
        account_name=account_name,
        account_key=account_key,
    )

    entries = filesystem.find(f"{container}/{path}")
    if not entries:
        print(f"FAIL: nothing found under {bucket_url}")
        return 1

    total = 0
    for entry in entries:
        size = filesystem.info(entry).get("size", 0)
        total += size
        print(f"  {entry}  ({size / 1e6:,.2f} MB)")
    print(f"\nFiles in Azure: {len(entries)}  |  total {total / 1e6:,.1f} MB")
    return 0


def main() -> int:
    cfg.banner("03 | MinIO (Iceberg) -> Azure ADLS")

    if cfg.GROUP_FOLDER == "GRUPO_X":
        print("FAIL: GROUP_FOLDER is still the placeholder 'GRUPO_X'.")
        print("Set the real folder assigned to your group in TWO places:")
        print("  1. .env                -> GROUP_FOLDER=<your folder>")
        print("  2. dlt/secrets.toml    -> [minio_to_azure.destination.filesystem]")
        print("                            bucket_url = \"abfss://clase-4-dlt@fhbd.dfs.core.windows.net/<your folder>\"")
        return 1

    bucket_url = secret(f"{PIPELINE_NAME}.destination.filesystem.bucket_url")
    if not bucket_url:
        print("FAIL: bucket_url missing from dlt/secrets.toml under")
        print(f"      [{PIPELINE_NAME}.destination.filesystem]")
        return 1
    if "GRUPO_X" in bucket_url:
        print(f"FAIL: bucket_url still contains the placeholder: {bucket_url}")
        return 1

    source_prefix = iceberg_data_prefix()
    print(f"Source     : {source_prefix}")
    print(f"Destination: {bucket_url}")

    reader = readers(bucket_url=source_prefix, file_glob="**/*.parquet").read_parquet()
    reader = reader.with_name(cfg.ICEBERG_TABLE)

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="filesystem",
        dataset_name=cfg.NESSIE_NAMESPACE,
    )

    load_info = pipeline.run(
        reader,
        loader_file_format="parquet",
        write_disposition="replace",
    )
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)

    cfg.banner("Verification | files in Azure ADLS")
    return verify_in_azure(bucket_url)


if __name__ == "__main__":
    sys.exit(main())
