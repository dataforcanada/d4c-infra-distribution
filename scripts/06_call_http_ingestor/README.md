# 06 – Call HTTP Ingestor

Python CLI that orchestrates concurrent calls to the [Cloudflare HTTP ingestor worker](../05_cloudflare_http_ingestor/) and persists results to SQLite + Parquet.

## Quick start

```bash
cd scripts/06_call_http_ingestor

# Install & run (uv handles the virtualenv automatically)
uv run d4c-http-ingestor \
  --urls ../05_cloudflare_http_ingestor/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec_2026-03-10.txt \
  --dataset-id ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec \
  --worker-url https://cf-data-ingestor.labs.dataforcanada.org/ \
  --auth-token "$D4C_INGESTOR_AUTH_TOKEN" \
  --db ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec.sqlite \
  --key-prefix dataforcanada/d4c-datapkg-orthoimagery/archive/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec \
  --out ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec \
  --concurrency 12
```

The auth token can also be set via the `D4C_INGESTOR_AUTH_TOKEN` environment variable.

## CLI reference

```
usage: d4c-http-ingestor [-h] --urls URLS --dataset-id DATASET_ID
                         [--worker-url WORKER_URL] [--auth-token AUTH_TOKEN]
                         --db DB [--key-prefix KEY_PREFIX] --out OUT
                         [--concurrency CONCURRENCY] [--timeout TIMEOUT]
                         [--max-retries MAX_RETRIES]
                         [--resume | --no-resume] [--force-refresh]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--urls` | *(required)* | Path to a newline-delimited file of URLs to ingest |
| `--dataset-id` | *(required)* | Logical dataset identifier (used in User-Agent and DB) |
| `--worker-url` | `https://cf-data-ingestor.labs.dataforcanada.org/` | Base URL of the Cloudflare ingestor worker |
| `--auth-token` | `$D4C_INGESTOR_AUTH_TOKEN` | Bearer token for the worker |
| `--db` | *(required)* | Path to the SQLite database file |
| `--key-prefix` | `""` | S3 key prefix passed to the worker |
| `--out` | *(required)* | Parquet output filename stem (e.g. `my-dataset` → `my-dataset.parquet`) |
| `--concurrency` | `12` | Maximum concurrent worker requests |
| `--timeout` | `600` | Per-request timeout in seconds |
| `--max-retries` | `3` | Maximum retry attempts per URL on failure |
| `--resume` | `true` | Skip URLs already recorded as `success` |
| `--force-refresh` | `false` | Ignore cached freshness; re-process all URLs |

## How it works

1. Reads URLs from the input file.
2. Opens (or creates) a SQLite database with the `downloads` table.
3. If `--resume` (default), filters out URLs already marked `success`.
4. Submits up to `--concurrency` concurrent POST requests to the worker.
5. Each request sends:
   ```json
   {
     "download_url": "<url from file>",
     "user_agent": "Data for Canada - <dataset-id>",
     "key_prefix": "<key-prefix>"
   }
   ```
6. Persists each result (success/failed) to SQLite with idempotent upsert, including the ETag, multipart info, and timestamps returned by the worker.
7. Failed URLs are retried with exponential backoff + jitter (up to `--max-retries`).
8. **Every 100 successful downloads**, the full `downloads` table is exported to `{out}.parquet` and uploaded to S3 via the worker's PUT endpoint at the `--key-prefix` location.
9. On completion, a final Parquet export + upload is performed.

Re-runs append new datasets or update existing rows into the Parquet dataset.

## Data model

### SQLite schema (`downloads` table)

```sql
CREATE TABLE IF NOT EXISTS downloads (
  url                    TEXT PRIMARY KEY,
  dataset_id             TEXT NOT NULL,
  status                 TEXT NOT NULL,  -- success | failed | skipped
  http_status            INTEGER,
  etag                   TEXT,
  error                  TEXT,
  started_at             TEXT NOT NULL,
  finished_at            TEXT,
  multipart_part_size    INTEGER,
  multipart_number_parts INTEGER
);
CREATE INDEX IF NOT EXISTS ix_downloads_dataset ON downloads(dataset_id);
CREATE INDEX IF NOT EXISTS ix_downloads_status  ON downloads(status);
```

### Parquet columns

| Column | Arrow Type | Description |
|--------|-----------|-------------|
| `url` | `string` | Source download URL |
| `dataset_id` | `string` | Logical dataset identifier |
| `status` | `string` | `success`, `failed`, or `skipped` |
| `http_status` | `int32` | HTTP status code from the worker |
| `etag` | `string` | S3 ETag of the uploaded object (quotes stripped) |
| `error` | `string` | Error message (if failed) |
| `started_at` | `timestamp[us, tz=UTC]` | When the worker started processing (from worker response) |
| `finished_at` | `timestamp[us, tz=UTC]` | When the worker finished processing (from worker response) |
| `multipart_part_size` | `int32` | S3 multipart part size in bytes (if multipart was used) |
| `multipart_number_parts` | `int32` | Number of parts uploaded (if multipart was used) |

> Note: `started_at` and `finished_at` are stored as ISO-8601 text in SQLite but converted to proper Arrow timestamps in the Parquet output. These values come from the Cloudflare worker response, not the Python CLI.

## Dependencies

- [httpx](https://www.python-httpx.org/) – async HTTP client
- [pyarrow](https://arrow.apache.org/docs/python/) – Parquet I/O
- [rich](https://rich.readthedocs.io/) – progress bars and terminal output
