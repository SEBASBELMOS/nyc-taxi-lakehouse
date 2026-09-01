"""01 - HTTP source -> MinIO RAW bucket (Parquet), loaded with dlt.

Requirement RF-01: read the official NYC yellow taxi Parquet over HTTP and
land it in a MinIO bucket, using dlt for the ingestion.

Memory note: the source file holds ~3.5M rows. Reading it whole into a pandas
DataFrame (the shortest way to write this) peaks at several GB inside the
container. Instead the file is downloaded once and then handed to dlt in
PyArrow record batches, which keeps memory flat and lets dlt write Parquet
directly without a row-by-row normalization pass.

Run:  python scripts/01_http_to_minio.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
import requests

import config as cfg

PIPELINE_NAME = "http_to_minio"


def download(url: str, destination: Path) -> Path:
    """Stream the source file to local disk.

    Downloading first (instead of reading the URL through fsspec) avoids
    hundreds of HTTP range requests while PyArrow seeks around the Parquet
    footer and row groups.
    """
    print(f"Downloading {url}", flush=True)
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        total = int(response.headers.get("Content-Length", 0))
        written = 0
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                handle.write(chunk)
                written += len(chunk)
                if total:
                    print(f"  {written / total:6.1%}  ({written / 1e6:,.0f} MB)", flush=True)
    print(f"Downloaded {destination.stat().st_size / 1e6:,.1f} MB", flush=True)
    return destination


@dlt.resource(name=cfg.RAW_TABLE, write_disposition="replace")
def yellow_tripdata():
    """Yield the source Parquet as PyArrow batches.

    `write_disposition="replace"` matters: it makes the script idempotent, so
    re-running it does not duplicate rows in the RAW bucket.
    """
    with tempfile.TemporaryDirectory() as tmp:
        local = download(cfg.SOURCE_URL, Path(tmp) / "source.parquet")

        parquet_file = pq.ParquetFile(local)
        total_rows = parquet_file.metadata.num_rows
        print(f"Source rows: {total_rows:,}", flush=True)

        emitted = 0
        for batch in parquet_file.iter_batches(batch_size=cfg.BATCH_ROWS):
            emitted += batch.num_rows
            print(f"  -> {emitted:,} / {total_rows:,} rows", flush=True)
            yield pa.Table.from_batches([batch])


def main() -> int:
    cfg.banner("01 | HTTP -> MinIO (RAW bucket)")
    print(f"Source     : {cfg.SOURCE_URL}")
    print(f"Destination: s3://{cfg.RAW_BUCKET}/{cfg.RAW_DATASET}/{cfg.RAW_TABLE}/")
    print(f"MinIO      : {cfg.MINIO_ENDPOINT}")

    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="filesystem",
        dataset_name=cfg.RAW_DATASET,
    )

    load_info = pipeline.run(yellow_tripdata(), loader_file_format="parquet")
    print(load_info)

    cfg.banner("Verification | objects in the RAW bucket")
    filesystem = cfg.minio_filesystem()
    prefix = f"{cfg.RAW_BUCKET}/{cfg.RAW_DATASET}/{cfg.RAW_TABLE}"
    objects = cfg.list_parquet(filesystem, prefix)

    if not objects:
        print(f"FAIL: no Parquet file found under s3://{prefix}/")
        return 1

    rows = 0
    for key in objects:
        size = filesystem.info(key)["size"]
        with filesystem.open(key, "rb") as handle:
            rows += pq.ParquetFile(handle).metadata.num_rows
        print(f"  {key}  ({size / 1e6:,.1f} MB)")

    print(f"\nRows landed in MinIO: {rows:,}")
    if rows != cfg.EXPECTED_ROWS:
        print(f"WARNING: expected {cfg.EXPECTED_ROWS:,} rows, found {rows:,}")
        return 1

    print("OK - RAW bucket matches the expected row count.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
