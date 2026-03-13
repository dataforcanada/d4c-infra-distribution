"""SQLite persistence layer for download tracking."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class DownloadRow:
    """Mirrors a single row in the ``downloads`` table."""

    url: str
    dataset_id: str
    status: str  # success | failed | skipped
    http_status: int | None = None
    etag: str | None = None
    error: str | None = None
    started_at: str = ""
    finished_at: str | None = None
    multipart_part_size: int | None = None
    multipart_number_parts: int | None = None


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS downloads (
  url                    TEXT PRIMARY KEY,
  dataset_id             TEXT NOT NULL,
  status                 TEXT NOT NULL,
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
"""


# ---------------------------------------------------------------------------
# Database handle
# ---------------------------------------------------------------------------

class DownloadsDB:
    """Thin wrapper around a SQLite database for download tracking."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA_SQL)

    # -- queries -------------------------------------------------------------

    def get(self, url: str) -> DownloadRow | None:
        """Return the row for *url*, or ``None`` if it doesn't exist."""
        cur = self._conn.execute("SELECT * FROM downloads WHERE url = ?", (url,))
        row = cur.fetchone()
        if row is None:
            return None
        return DownloadRow(**dict(row))

    def successful_urls(self) -> set[str]:
        """Return the set of URLs already recorded as ``success``."""
        cur = self._conn.execute(
            "SELECT url FROM downloads WHERE status = 'success'"
        )
        return {r["url"] for r in cur.fetchall()}

    def pending_urls(self) -> list[str]:
        """Return URLs recorded as ``failed`` or not yet present (for resume)."""
        cur = self._conn.execute(
            "SELECT url FROM downloads WHERE status IN ('failed')"
        )
        return [r["url"] for r in cur.fetchall()]

    def count_successful(self) -> int:
        """Return the number of rows with status ``success``."""
        cur = self._conn.execute(
            "SELECT COUNT(*) AS cnt FROM downloads WHERE status = 'success'"
        )
        return cur.fetchone()["cnt"]

    # -- mutations -----------------------------------------------------------

    def upsert(self, row: DownloadRow) -> None:
        """Insert or replace a download row."""
        self._conn.execute(
            """\
            INSERT INTO downloads (
                url, dataset_id, status, http_status, etag, error,
                started_at, finished_at,
                multipart_part_size, multipart_number_parts
            )
            VALUES (
                :url, :dataset_id, :status, :http_status, :etag, :error,
                :started_at, :finished_at,
                :multipart_part_size, :multipart_number_parts
            )
            ON CONFLICT(url) DO UPDATE SET
                dataset_id             = excluded.dataset_id,
                status                 = excluded.status,
                http_status            = excluded.http_status,
                etag                   = excluded.etag,
                error                  = excluded.error,
                started_at             = excluded.started_at,
                finished_at            = excluded.finished_at,
                multipart_part_size    = excluded.multipart_part_size,
                multipart_number_parts = excluded.multipart_number_parts
            """,
            row.__dict__,
        )
        self._conn.commit()

    def all_rows(self) -> list[DownloadRow]:
        """Return every row in the table."""
        cur = self._conn.execute("SELECT * FROM downloads ORDER BY started_at")
        return [DownloadRow(**dict(r)) for r in cur.fetchall()]

    def count_by_status(self) -> dict[str, int]:
        """Return ``{status: count}`` summary."""
        cur = self._conn.execute(
            "SELECT status, COUNT(*) AS cnt FROM downloads GROUP BY status"
        )
        return {r["status"]: r["cnt"] for r in cur.fetchall()}

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DownloadsDB":
        return self

    def __exit__(self, *exc) -> None:  # noqa: ANN002
        self.close()
