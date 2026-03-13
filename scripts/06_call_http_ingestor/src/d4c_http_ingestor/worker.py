"""HTTP client for the Cloudflare data-ingestor worker."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


@dataclass
class WorkerResult:
    """Outcome of a single worker invocation."""

    url: str
    ok: bool
    http_status: int | None = None
    bucket: str | None = None
    key: str | None = None
    content_type: str | None = None
    size_bytes: int | None = None
    etag: str | None = None
    error: str | None = None
    multipart_part_size: int | None = None
    multipart_number_parts: int | None = None
    started_at: str | None = None
    finished_at: str | None = None


async def call_worker(
    client: httpx.AsyncClient,
    *,
    worker_url: str,
    auth_token: str,
    download_url: str,
    user_agent: str,
    key_prefix: str,
    timeout: float = 600.0,
) -> WorkerResult:
    """POST a single download job to the Cloudflare worker.

    Returns a :class:`WorkerResult` regardless of success/failure so the
    caller never has to catch transport exceptions.
    """
    payload = {
        "download_url": download_url,
        "user_agent": user_agent,
        "key_prefix": key_prefix,
    }
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
    }

    started = datetime.now(timezone.utc).isoformat()

    try:
        resp = await client.post(
            worker_url,
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        body: dict[str, Any] = resp.json()

        if resp.is_success and body.get("ok"):
            return WorkerResult(
                url=download_url,
                ok=True,
                http_status=resp.status_code,
                bucket=body.get("bucket"),
                key=body.get("key"),
                content_type=body.get("content_type"),
                size_bytes=body.get("size_bytes"),
                etag=body.get("etag"),
                multipart_part_size=body.get("multipart_part_size"),
                multipart_number_parts=body.get("multipart_number_parts"),
                started_at=body.get("started_at"),
                finished_at=body.get("finished_at"),
            )
        else:
            return WorkerResult(
                url=download_url,
                ok=False,
                http_status=resp.status_code,
                error=body.get("error", resp.text),
                started_at=body.get("started_at"),
                finished_at=body.get("finished_at"),
            )
    except httpx.TimeoutException as exc:
        return WorkerResult(url=download_url, ok=False, error=f"Timeout: {exc}", started_at=started)
    except httpx.HTTPError as exc:
        return WorkerResult(url=download_url, ok=False, error=f"HTTP error: {exc}", started_at=started)
    except Exception as exc:  # noqa: BLE001
        return WorkerResult(url=download_url, ok=False, error=str(exc), started_at=started)


async def call_worker_with_retries(
    client: httpx.AsyncClient,
    *,
    worker_url: str,
    auth_token: str,
    download_url: str,
    user_agent: str,
    key_prefix: str,
    timeout: float = 600.0,
    max_retries: int = 3,
    backoff_base: float = 2.0,
    backoff_max: float = 60.0,
) -> WorkerResult:
    """Call the worker with exponential backoff + jitter on failure."""
    last_result: WorkerResult | None = None

    for attempt in range(1, max_retries + 1):
        result = await call_worker(
            client,
            worker_url=worker_url,
            auth_token=auth_token,
            download_url=download_url,
            user_agent=user_agent,
            key_prefix=key_prefix,
            timeout=timeout,
        )
        if result.ok:
            return result

        last_result = result

        if attempt < max_retries:
            delay = min(backoff_base ** attempt, backoff_max)
            jitter = random.uniform(0, delay * 0.5)  # noqa: S311
            await asyncio.sleep(delay + jitter)

    assert last_result is not None  # noqa: S101
    return last_result


async def upload_file_to_worker(
    client: httpx.AsyncClient,
    *,
    worker_url: str,
    auth_token: str,
    file_path: Path,
    s3_key: str,
    content_type: str = "application/octet-stream",
    timeout: float = 120.0,
) -> WorkerResult:
    """Upload a local file directly to S3 via the worker PUT endpoint.

    Reads the file and sends it as a PUT request body with the ``X-S3-Key``
    header specifying the destination S3 object key.
    """
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "X-S3-Key": s3_key,
        "Content-Type": content_type,
    }

    try:
        file_size = file_path.stat().st_size
        headers["Content-Length"] = str(file_size)

        with open(file_path, "rb") as fh:
            file_bytes = fh.read()

        resp = await client.put(
            worker_url,
            content=file_bytes,
            headers=headers,
            timeout=timeout,
        )
        body: dict[str, Any] = resp.json()

        if resp.is_success and body.get("ok"):
            return WorkerResult(
                url=str(file_path),
                ok=True,
                http_status=resp.status_code,
                bucket=body.get("bucket"),
                key=body.get("key"),
                content_type=body.get("content_type"),
                size_bytes=body.get("size_bytes"),
                etag=body.get("etag"),
                multipart_part_size=body.get("multipart_part_size"),
                multipart_number_parts=body.get("multipart_number_parts"),
                started_at=body.get("started_at"),
                finished_at=body.get("finished_at"),
            )
        else:
            return WorkerResult(
                url=str(file_path),
                ok=False,
                http_status=resp.status_code,
                error=body.get("error", resp.text),
                started_at=body.get("started_at"),
                finished_at=body.get("finished_at"),
            )
    except httpx.TimeoutException as exc:
        return WorkerResult(url=str(file_path), ok=False, error=f"Timeout: {exc}")
    except httpx.HTTPError as exc:
        return WorkerResult(url=str(file_path), ok=False, error=f"HTTP error: {exc}")
    except Exception as exc:  # noqa: BLE001
        return WorkerResult(url=str(file_path), ok=False, error=str(exc))
