"""SQLite persistence layer for download tracking."""

from __future__ import annotations

import sqlite3
import sys
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

# Columns in the canonical schema (order matters for the migration INSERT).
_CANONICAL_COLUMNS = [
    "url", "dataset_id", "status", "http_status", "etag", "error",
    "started_at", "finished_at", "multipart_part_size", "multipart_number_parts",
]


# ---------------------------------------------------------------------------
# Migration helpers
# ---------------------------------------------------------------------------

def _table_has_primary_key(conn: sqlite3.Connection, table: str) -> bool:
    """Return ``True`` if *table* has at least one column marked as PK."""
    cur = conn.execute(f"PRAGMA table_info({table})")
    for row in cur.fetchall():
        if row["pk"]:  # pk column is non-zero for PK members
            return True
    return False


def _existing_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    """Return the column names of *table* in definition order."""
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row["name"] for row in cur.fetchall()]


def _migrate_downloads(conn: sqlite3.Connection) -> None:
    """Recreate the ``downloads`` table with the correct PRIMARY KEY.

    This handles databases that were originally created by an external tool
    (e.g. pandas / DuckDB) where ``url`` was defined as plain ``VARCHAR``
    without a PRIMARY KEY constraint.  Existing data is preserved; duplicate
    ``url`` values (if any) are collapsed by keeping the most recent row
    (highest ``rowid``).
    """
    old_cols = _existing_columns(conn, "downloads")
    # Only copy columns that exist in both old and new schemas.
    shared = [c for c in _CANONICAL_COLUMNS if c in old_cols]

    cols_csv = ", ".join(shared)

    conn.executescript(f"""\
        BEGIN;
        ALTER TABLE downloads RENAME TO _downloads_old;
        CREATE TABLE downloads (
          url                    TEXT PRIMARY KEY,
          dataset_id             TEXT NOT NULL DEFAULT '',
          status                 TEXT NOT NULL DEFAULT '',
          http_status            INTEGER,
          etag                   TEXT,
          error                  TEXT,
          started_at             TEXT NOT NULL DEFAULT '',
          finished_at            TEXT,
          multipart_part_size    INTEGER,
          multipart_number_parts INTEGER
        );
        INSERT OR IGNORE INTO downloads ({cols_csv})
            SELECT {cols_csv}
            FROM _downloads_old
            ORDER BY rowid DESC;
        DROP TABLE _downloads_old;
        CREATE INDEX IF NOT EXISTS ix_downloads_dataset ON downloads(dataset_id);
        CREATE INDEX IF NOT EXISTS ix_downloads_status  ON downloads(status);
        COMMIT;
    """)
    print(
        f"[migrate] Recreated 'downloads' table with PRIMARY KEY(url); "
        f"copied columns: {cols_csv}",
        file=sys.stderr,
    )


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

        # --- Schema migration ---------------------------------------------------
        # If the table already exists but was created without a PRIMARY KEY
        # (e.g. by pandas/DuckDB), recreate it with the correct schema.
        cur = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='downloads'"
        )
        if cur.fetchone() is not None:
            if not _table_has_primary_key(self._conn, "downloads"):
                print(
                    "[migrate] Detected 'downloads' table without PRIMARY KEY — migrating…",
                    file=sys.stderr,
                )
                _migrate_downloads(self._conn)
        # ------------------------------------------------------------------------

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
