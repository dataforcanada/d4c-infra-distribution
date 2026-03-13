"""Export the downloads table to a Parquet dataset."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from d4c_http_ingestor.db import DownloadRow

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_iso(s: str | None) -> datetime | None:
    """Parse an ISO-8601 string into a datetime, or return None."""
    if not s:
        return None
    return datetime.fromisoformat(s)


# ---------------------------------------------------------------------------
# Schema – mirrors the SQLite ``downloads`` table
# ---------------------------------------------------------------------------

_ARROW_SCHEMA = pa.schema(
    [
        pa.field("url", pa.string(), nullable=False),
        pa.field("dataset_id", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("http_status", pa.int32(), nullable=True),
        pa.field("etag", pa.string(), nullable=True),
        pa.field("error", pa.string(), nullable=True),
        pa.field("started_at", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("finished_at", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("multipart_part_size", pa.int32(), nullable=True),
        pa.field("multipart_number_parts", pa.int32(), nullable=True),
    ]
)


def rows_to_table(rows: list[DownloadRow]) -> pa.Table:
    """Convert a list of :class:`DownloadRow` into a PyArrow Table."""
    arrays = [
        pa.array([r.url for r in rows], type=pa.string()),
        pa.array([r.dataset_id for r in rows], type=pa.string()),
        pa.array([r.status for r in rows], type=pa.string()),
        pa.array([r.http_status for r in rows], type=pa.int32()),
        pa.array([r.etag for r in rows], type=pa.string()),
        pa.array([r.error for r in rows], type=pa.string()),
        pa.array(
            [_parse_iso(r.started_at) for r in rows],
            type=pa.timestamp("us", tz="UTC"),
        ),
        pa.array(
            [_parse_iso(r.finished_at) for r in rows],
            type=pa.timestamp("us", tz="UTC"),
        ),
        pa.array([r.multipart_part_size for r in rows], type=pa.int32()),
        pa.array([r.multipart_number_parts for r in rows], type=pa.int32()),
    ]
    return pa.table(arrays, schema=_ARROW_SCHEMA)


def export_parquet(rows: list[DownloadRow], out_stem: str) -> Path:
    """Write *rows* as a Parquet file named ``{out_stem}.parquet`` in the CWD.

    The file is overwritten on each call so that re-runs always reflect the
    latest state of the SQLite database.

    Returns the path to the written file.
    """
    dest = Path(f"{out_stem}.parquet")
    table = rows_to_table(rows)
    pq.write_table(table, dest, compression="zstd")
    return dest
