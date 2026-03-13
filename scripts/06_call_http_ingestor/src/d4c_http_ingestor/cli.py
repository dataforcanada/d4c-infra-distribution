"""CLI entry-point for d4c-http-ingestor.

Orchestrates concurrent calls to the Cloudflare HTTP ingestor worker,
persists results to SQLite, and exports a Parquet artifact.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from d4c_http_ingestor.db import DownloadRow, DownloadsDB
from d4c_http_ingestor.parquet import export_parquet
from d4c_http_ingestor.worker import call_worker_with_retries, upload_file_to_worker

console = Console()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EXPORT_EVERY_N = 100  # Export + upload Parquet every N successful downloads

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_urls(path: str) -> list[str]:
    """Read a newline-delimited URL file, stripping blanks and comments."""
    lines: list[str] = []
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if line and not line.startswith("#"):
                lines.append(line)
    return lines


# ---------------------------------------------------------------------------
# Async orchestrator
# ---------------------------------------------------------------------------


async def _export_and_upload(
    db: DownloadsDB,
    client: httpx.AsyncClient,
    *,
    out_stem: str,
    worker_url: str,
    auth_token: str,
    key_prefix: str,
) -> None:
    """Export the SQLite DB to Parquet and upload it to S3 via the worker."""
    rows = db.all_rows()
    if not rows:
        return

    parquet_path = export_parquet(rows, out_stem)
    parquet_filename = parquet_path.name
    s3_key = (
        f"{key_prefix.rstrip('/')}/{parquet_filename}"
        if key_prefix
        else parquet_filename
    )

    console.print(
        f"  [cyan]Uploading[/] {parquet_filename} → s3://…/{s3_key}"
    )
    result = await upload_file_to_worker(
        client,
        worker_url=worker_url,
        auth_token=auth_token,
        file_path=parquet_path,
        s3_key=s3_key,
        content_type="application/vnd.apache.parquet",
    )
    if result.ok:
        console.print(
            f"  [green]✓[/] Parquet uploaded ({len(rows)} rows, "
            f"{parquet_path.stat().st_size:,} bytes)"
        )
    else:
        console.print(
            f"  [red]✗[/] Parquet upload failed: {result.error}"
        )


async def _process_urls(
    urls: list[str],
    *,
    db: DownloadsDB,
    dataset_id: str,
    worker_url: str,
    auth_token: str,
    key_prefix: str,
    out_stem: str,
    concurrency: int,
    timeout: float,
    max_retries: int,
    progress: Progress,
    task_id: int,
) -> None:
    """Submit *urls* to the worker with bounded concurrency.

    Uses a fixed-size worker pool so that exactly *concurrency* requests
    are in-flight at any time.  As soon as one request completes, the
    next URL is picked up immediately — no idle slots.

    Every :data:`EXPORT_EVERY_N` successful downloads, the SQLite database
    is exported to Parquet and uploaded to S3 via the worker's PUT endpoint.
    """
    queue: asyncio.Queue[str | None] = asyncio.Queue()

    # Seed the queue with every URL to process.
    for url in urls:
        queue.put_nowait(url)

    # Sentinel values – one per worker – so they know when to stop.
    for _ in range(concurrency):
        queue.put_nowait(None)

    # Shared mutable state protected by a lock.
    success_count = 0
    export_lock = asyncio.Lock()

    async def _worker(client: httpx.AsyncClient) -> None:
        """Pull URLs from the queue until a ``None`` sentinel is received."""
        nonlocal success_count

        while True:
            url = await queue.get()
            if url is None:
                return

            user_agent = f"Data for Canada - {dataset_id}"

            result = await call_worker_with_retries(
                client,
                worker_url=worker_url,
                auth_token=auth_token,
                download_url=url,
                user_agent=user_agent,
                key_prefix=key_prefix,
                timeout=timeout,
                max_retries=max_retries,
            )

            # Use started_at/finished_at from the worker response
            row = DownloadRow(
                url=url,
                dataset_id=dataset_id,
                status="success" if result.ok else "failed",
                http_status=result.http_status,
                etag=result.etag,
                error=result.error,
                started_at=result.started_at or "",
                finished_at=result.finished_at,
                multipart_part_size=result.multipart_part_size,
                multipart_number_parts=result.multipart_number_parts,
            )
            db.upsert(row)
            progress.advance(task_id)

            # Periodic Parquet export + upload every N successes
            if result.ok:
                async with export_lock:
                    success_count += 1
                    if success_count % EXPORT_EVERY_N == 0:
                        console.print(
                            f"\n  [yellow]Checkpoint[/]: {success_count} "
                            f"successes — exporting Parquet…"
                        )
                        await _export_and_upload(
                            db,
                            client,
                            out_stem=out_stem,
                            worker_url=worker_url,
                            auth_token=auth_token,
                            key_prefix=key_prefix,
                        )

    # Use a single shared httpx client with generous limits.
    limits = httpx.Limits(
        max_connections=concurrency + 4,
        max_keepalive_connections=concurrency,
    )
    async with httpx.AsyncClient(limits=limits, follow_redirects=True) as client:
        workers = [asyncio.create_task(_worker(client)) for _ in range(concurrency)]
        await asyncio.gather(*workers)

        # Final export + upload after all URLs are processed
        console.print("\n  [yellow]Final export[/]: exporting Parquet…")
        await _export_and_upload(
            db,
            client,
            out_stem=out_stem,
            worker_url=worker_url,
            auth_token=auth_token,
            key_prefix=key_prefix,
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="d4c-http-ingestor",
        description="Orchestrate concurrent downloads via the Cloudflare HTTP ingestor worker.",
    )
    p.add_argument(
        "--urls",
        required=True,
        help="Path to a newline-delimited file of URLs to ingest.",
    )
    p.add_argument(
        "--dataset-id",
        required=True,
        help="Logical dataset identifier (used in User-Agent and DB).",
    )
    p.add_argument(
        "--worker-url",
        default="https://cf-data-ingestor.labs.dataforcanada.org/",
        help="Base URL of the Cloudflare ingestor worker.",
    )
    p.add_argument(
        "--auth-token",
        default=os.environ.get("D4C_INGESTOR_AUTH_TOKEN", ""),
        help="Bearer token for the worker (default: $D4C_INGESTOR_AUTH_TOKEN).",
    )
    p.add_argument(
        "--db",
        required=True,
        help="Path to the SQLite database file.",
    )
    p.add_argument(
        "--key-prefix",
        default="",
        help="S3 key prefix passed to the worker (e.g. dataforcanada/d4c-datapkg-orthoimagery/archive/).",
    )
    p.add_argument(
        "--out",
        required=True,
        help=(
            "Parquet output filename stem. For example, "
            "'--out my-dataset' creates 'my-dataset.parquet' in the "
            "current working directory."
        ),
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=12,
        help="Maximum number of concurrent worker requests (default: 12).",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=600.0,
        help="Per-request timeout in seconds (default: 600).",
    )
    p.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts per URL on failure (default: 3).",
    )
    p.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume from previous run, skipping successful URLs (default: true).",
    )
    p.add_argument(
        "--force-refresh",
        action="store_true",
        default=False,
        help="Ignore cached freshness; re-process all URLs.",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.auth_token:
        console.print(
            "[bold red]Error:[/] --auth-token or $D4C_INGESTOR_AUTH_TOKEN is required."
        )
        sys.exit(1)

    # -- Read URL list -------------------------------------------------------
    url_file = Path(args.urls)
    if not url_file.is_file():
        console.print(f"[bold red]Error:[/] URL file not found: {url_file}")
        sys.exit(1)

    all_urls = _read_urls(str(url_file))
    console.print(f"Loaded [bold]{len(all_urls)}[/] URLs from [cyan]{url_file}[/]")

    # -- Open DB & determine work set ----------------------------------------
    with DownloadsDB(args.db) as db:
        if args.force_refresh:
            urls_to_process = all_urls
            console.print("[yellow]--force-refresh[/]: re-processing all URLs")
        elif args.resume:
            already_done = db.successful_urls()
            urls_to_process = [u for u in all_urls if u not in already_done]
            skipped = len(all_urls) - len(urls_to_process)
            if skipped:
                console.print(
                    f"Resuming: skipping [green]{skipped}[/] already-successful URLs"
                )
        else:
            urls_to_process = all_urls

        if not urls_to_process:
            console.print("[green]Nothing to do – all URLs already succeeded.[/]")
        else:
            console.print(
                f"Processing [bold]{len(urls_to_process)}[/] URLs "
                f"with concurrency=[cyan]{args.concurrency}[/]"
            )

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task_id = progress.add_task(
                    "Ingesting…", total=len(urls_to_process)
                )
                asyncio.run(
                    _process_urls(
                        urls_to_process,
                        db=db,
                        dataset_id=args.dataset_id,
                        worker_url=args.worker_url,
                        auth_token=args.auth_token,
                        key_prefix=args.key_prefix,
                        out_stem=args.out,
                        concurrency=args.concurrency,
                        timeout=args.timeout,
                        max_retries=args.max_retries,
                        progress=progress,
                        task_id=task_id,
                    )
                )

        # -- Summary ---------------------------------------------------------
        counts = db.count_by_status()
        console.print("\n[bold]Summary:[/]")
        for status, cnt in sorted(counts.items()):
            colour = {"success": "green", "failed": "red", "skipped": "yellow"}.get(
                status, "white"
            )
            console.print(f"  [{colour}]{status}[/]: {cnt}")

        # -- Final local Parquet export (no upload — already done above) -----
        rows = db.all_rows()
        if rows:
            dest = export_parquet(rows, args.out)
            console.print(f"\nParquet written to [cyan]{dest}[/] ({len(rows)} rows)")
        else:
            console.print("\n[yellow]No rows to export.[/]")

    console.print("[bold green]Done.[/]")


if __name__ == "__main__":
    main()
