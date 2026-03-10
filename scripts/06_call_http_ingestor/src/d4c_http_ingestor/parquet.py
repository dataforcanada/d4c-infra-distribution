"""Export the downloads table to a Parquet dataset."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from d4c_http_ingestor.db import DownloadRow

# ---------------------------------------------------------------------------
# Schema – mirrors the SQLite ``downloads`` table
# ---------------------------------------------------------------------------

_ARROW_SCHEMA = pa.schema(
    [
        pa.field("url", pa.string(), nullable=False),
        pa.field("dataset_id", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("http_status", pa.int32(), nullable=True),
        pa.field("error", pa.string(), nullable=True),
        pa.field("started_at", pa.string(), nullable=False),
        pa.field("finished_at", pa.string(), nullable=True),
    ]
)


def rows_to_table(rows: list[DownloadRow]) -> pa.Table:
    """Convert a list of :class:`DownloadRow` into a PyArrow Table."""
    arrays = [
        pa.array([r.url for r in rows], type=pa.string()),
        pa.array([r.dataset_id for r in rows], type=pa.string()),
        pa.array([r.status for r in rows], type=pa.string()),
        pa.array([r.http_status for r in rows], type=pa.int32()),
        pa.array([r.error for r in rows], type=pa.string()),
        pa.array([r.started_at for r in rows], type=pa.string()),
        pa.array([r.finished_at for r in rows], type=pa.string()),
    ]
    return pa.table(arrays, schema=_ARROW_SCHEMA)


def export_parquet(rows: list[DownloadRow], out_dir: str | Path) -> Path:
    """Write *rows* as a single Parquet file inside *out_dir*.

    The file is named ``downloads.parquet`` and is overwritten on each run so
    that re-runs always reflect the latest state of the SQLite database.

    Returns the path to the written file.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / "downloads.parquet"

    table = rows_to_table(rows)
    pq.write_table(table, dest, compression="zstd")
    return dest
