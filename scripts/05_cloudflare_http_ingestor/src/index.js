import { AwsClient } from "aws4fetch";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB – S3 minimum for multipart parts
const MAX_SINGLE_PUT_SIZE = 100 * 1024 * 1024; // 100 MiB – above this, always use multipart
const MAX_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 1000; // 1 second, doubles each retry

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Return a plain-text Response with the given status. */
function textResponse(body, status = 200) {
  return new Response(body, {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}

/** Return a JSON Response. */
function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/** Derive the S3 object key from the download URL (filename only). */
function objectKeyFromUrl(downloadUrl, prefix) {
  const pathname = new URL(downloadUrl).pathname;
  const filename = pathname.split("/").pop() || "unnamed";
  return prefix ? `${prefix.replace(/\/+$/, "")}/${filename}` : filename;
}

/** Build a pre-configured AwsClient for S3 in the target region. */
function makeAwsClient(env) {
  return new AwsClient({
    accessKeyId: env.AWS_ACCESS_KEY_ID,
    secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
    region: env.S3_REGION || "us-west-2",
    service: "s3",
  });
}

/**
 * Construct the S3 endpoint URL for a given bucket + key.
 * Uses path-style addressing (s3.region.amazonaws.com/bucket/key) which is
 * required for bucket names containing dots (e.g. "us-west-2.opendata.source.coop").
 * If a custom S3_ENDPOINT is set, it is used as the base instead.
 */
function s3Url(bucket, key, region, endpoint) {
  let base;
  if (endpoint) {
    // Normalise: strip trailing slashes, prepend https:// if no scheme given
    base = endpoint.replace(/\/+$/, "");
    if (!/^https?:\/\//i.test(base)) {
      base = `https://${base}`;
    }
  } else {
    base = `https://s3.${region}.amazonaws.com`;
  }
  return `${base}/${bucket}/${encodeURI(key)}`;
}

/** Sleep for the given number of milliseconds. */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Determine if an error is retryable (transient network / server errors).
 */
function isRetryable(err) {
  const msg = (err.message || "").toLowerCase();
  // Network-level failures
  if (
    msg.includes("network") ||
    msg.includes("connection") ||
    msg.includes("timeout") ||
    msg.includes("socket") ||
    msg.includes("econnreset") ||
    msg.includes("fetch failed")
  ) {
    return true;
  }
  // S3 server-side errors (5xx)
  if (/\b5\d{2}\b/.test(msg)) {
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// S3 Upload – Single PUT (streaming, requires known Content-Length)
// ---------------------------------------------------------------------------

async function putObjectStreaming(aws, bucket, region, key, body, contentLength, contentType, endpoint) {
  const url = s3Url(bucket, key, region, endpoint);
  console.log(`[putObjectStreaming] URL: ${url}`);
  console.log(`[putObjectStreaming] Content-Type: ${contentType}, Content-Length: ${contentLength}`);

  const headers = {
    "Content-Type": contentType || "application/octet-stream",
    "Content-Length": String(contentLength),
    "x-amz-acl": "bucket-owner-full-control",
    "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
  };

  console.log(`[putObjectStreaming] Sending PUT request...`);
  const resp = await aws.fetch(url, {
    method: "PUT",
    headers,
    body, // ReadableStream – streamed directly, no buffering
  });

  console.log(`[putObjectStreaming] Response status: ${resp.status}`);
  if (!resp.ok) {
    const text = await resp.text();
    console.error(`[putObjectStreaming] S3 PUT error body: ${text}`);
    throw new Error(`S3 PUT failed (${resp.status}): ${text}`);
  }
  console.log(`[putObjectStreaming] PUT succeeded`);
  return resp;
}

// ---------------------------------------------------------------------------
// S3 Upload – Multipart (streaming, for unknown Content-Length)
// ---------------------------------------------------------------------------

async function initiateMultipart(aws, bucket, region, key, contentType, endpoint) {
  const url = `${s3Url(bucket, key, region, endpoint)}?uploads`;
  console.log(`[initiateMultipart] URL: ${url}`);

  const resp = await aws.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": contentType || "application/octet-stream",
      "x-amz-acl": "bucket-owner-full-control",
      "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
    },
  });

  console.log(`[initiateMultipart] Response status: ${resp.status}`);
  if (!resp.ok) {
    const text = await resp.text();
    console.error(`[initiateMultipart] Error body: ${text}`);
    throw new Error(`Initiate multipart failed (${resp.status}): ${text}`);
  }
  const xml = await resp.text();
  const match = xml.match(/<UploadId>(.+?)<\/UploadId>/);
  if (!match) {
    console.error(`[initiateMultipart] Could not parse UploadId from XML: ${xml}`);
    throw new Error("Could not parse UploadId from response");
  }
  console.log(`[initiateMultipart] UploadId: ${match[1]}`);
  return match[1];
}

async function uploadPart(aws, bucket, region, key, uploadId, partNumber, body, length, endpoint) {
  const url = `${s3Url(bucket, key, region, endpoint)}?partNumber=${partNumber}&uploadId=${encodeURIComponent(uploadId)}`;
  console.log(`[uploadPart] Part ${partNumber}, size: ${length} bytes, URL: ${url}`);

  const resp = await aws.fetch(url, {
    method: "PUT",
    headers: {
      "Content-Length": String(length),
      "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
    },
    body,
  });

  console.log(`[uploadPart] Part ${partNumber} response status: ${resp.status}`);
  if (!resp.ok) {
    const text = await resp.text();
    console.error(`[uploadPart] Part ${partNumber} error body: ${text}`);
    throw new Error(`Upload part ${partNumber} failed (${resp.status}): ${text}`);
  }
  const etag = resp.headers.get("ETag");
  console.log(`[uploadPart] Part ${partNumber} ETag: ${etag}`);
  return etag;
}

/**
 * Upload a single part with retry logic for transient failures.
 * The body (Blob) can be re-read on each attempt.
 */
async function uploadPartWithRetry(aws, bucket, region, key, uploadId, partNumber, blob, length, endpoint) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Each retry needs a fresh body since the previous stream may be consumed
      const etag = await uploadPart(aws, bucket, region, key, uploadId, partNumber, blob, length, endpoint);
      return etag;
    } catch (err) {
      lastErr = err;
      if (attempt < MAX_RETRIES && isRetryable(err)) {
        const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1);
        console.warn(`[uploadPartWithRetry] Part ${partNumber} attempt ${attempt} failed (retryable): ${err.message}. Retrying in ${delay}ms...`);
        await sleep(delay);
      } else {
        console.error(`[uploadPartWithRetry] Part ${partNumber} attempt ${attempt} failed (non-retryable or max retries): ${err.message}`);
        break;
      }
    }
  }
  throw lastErr;
}

async function completeMultipart(aws, bucket, region, key, uploadId, parts, endpoint) {
  const partsXml = parts
    .map((p) => `<Part><PartNumber>${p.partNumber}</PartNumber><ETag>${p.etag}</ETag></Part>`)
    .join("");
  const xmlBody = `<CompleteMultipartUpload>${partsXml}</CompleteMultipartUpload>`;
  const url = `${s3Url(bucket, key, region, endpoint)}?uploadId=${encodeURIComponent(uploadId)}`;
  console.log(`[completeMultipart] URL: ${url}, parts: ${parts.length}`);

  const resp = await aws.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/xml" },
    body: xmlBody,
  });

  console.log(`[completeMultipart] Response status: ${resp.status}`);
  if (!resp.ok) {
    const text = await resp.text();
    console.error(`[completeMultipart] Error body: ${text}`);
    throw new Error(`Complete multipart failed (${resp.status}): ${text}`);
  }
  console.log(`[completeMultipart] Multipart upload completed successfully`);
  return resp;
}

async function abortMultipart(aws, bucket, region, key, uploadId, endpoint) {
  const url = `${s3Url(bucket, key, region, endpoint)}?uploadId=${encodeURIComponent(uploadId)}`;
  console.log(`[abortMultipart] Aborting upload ${uploadId}, URL: ${url}`);
  try {
    await aws.fetch(url, { method: "DELETE" });
    console.log(`[abortMultipart] Abort succeeded`);
  } catch (abortErr) {
    console.error(`[abortMultipart] Abort failed (best-effort): ${abortErr.message}`);
  }
}

/**
 * Read from a ReadableStream in ≥ MIN_PART_SIZE chunks and upload each as an
 * S3 multipart part.  Memory usage stays bounded to ~MIN_PART_SIZE at a time.
 */
async function multipartStreamUpload(aws, bucket, region, key, stream, contentType, endpoint) {
  console.log(`[multipartStreamUpload] Starting multipart upload for key: ${key}`);
  const uploadId = await initiateMultipart(aws, bucket, region, key, contentType, endpoint);
  const parts = [];
  let partNumber = 1;
  let buffer = [];
  let bufferSize = 0;
  let totalBytesRead = 0;

  const reader = stream.getReader();

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        console.log(`[multipartStreamUpload] Stream finished. Total bytes read: ${totalBytesRead}`);
        break;
      }

      buffer.push(value);
      bufferSize += value.byteLength;
      totalBytesRead += value.byteLength;

      // Flush when we've accumulated enough for a part
      if (bufferSize >= MIN_PART_SIZE) {
        console.log(`[multipartStreamUpload] Flushing part ${partNumber}, buffer size: ${bufferSize}`);
        const blob = new Blob(buffer);
        const etag = await uploadPartWithRetry(aws, bucket, region, key, uploadId, partNumber, blob, bufferSize, endpoint);
        parts.push({ partNumber, etag });
        partNumber++;
        buffer = [];
        bufferSize = 0;
      }
    }

    // Upload remaining bytes as the final part
    if (bufferSize > 0) {
      console.log(`[multipartStreamUpload] Uploading final part ${partNumber}, size: ${bufferSize}`);
      const blob = new Blob(buffer);
      const etag = await uploadPartWithRetry(aws, bucket, region, key, uploadId, partNumber, blob, bufferSize, endpoint);
      parts.push({ partNumber, etag });
    }

    await completeMultipart(aws, bucket, region, key, uploadId, parts, endpoint);
    console.log(`[multipartStreamUpload] Upload complete. Total parts: ${parts.length}, total bytes: ${totalBytesRead}`);
  } catch (err) {
    console.error(`[multipartStreamUpload] Upload failed at part ${partNumber}: ${err.message}`);
    console.error(`[multipartStreamUpload] Error stack: ${err.stack}`);
    await abortMultipart(aws, bucket, region, key, uploadId, endpoint);
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

export default {
  async fetch(request, env) {
    const requestId = crypto.randomUUID();
    console.log(`[handler] Request ${requestId} received: ${request.method} ${request.url}`);

    // ---- Method check ----
    if (request.method !== "POST") {
      console.log(`[handler] ${requestId} rejected: method ${request.method}`);
      return textResponse("Method Not Allowed. Use POST.", 405);
    }

    // ---- Auth check ----
    const authHeader = request.headers.get("Authorization") || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
    if (!token || token !== env.AUTH_TOKEN) {
      console.log(`[handler] ${requestId} rejected: unauthorized`);
      return textResponse("Unauthorized", 401);
    }

    // ---- Content-Type check ----
    const ct = request.headers.get("Content-Type") || "";
    if (!ct.includes("application/json")) {
      console.log(`[handler] ${requestId} rejected: Content-Type "${ct}"`);
      return textResponse("Content-Type must be application/json", 415);
    }

    // ---- Parse body ----
    let payload;
    try {
      payload = await request.json();
    } catch (parseErr) {
      console.error(`[handler] ${requestId} JSON parse error: ${parseErr.message}`);
      return textResponse("Invalid JSON body", 400);
    }

    const { download_url, user_agent, key_prefix } = payload;
    console.log(`[handler] ${requestId} payload: download_url=${download_url}, user_agent=${user_agent}, key_prefix=${key_prefix || "(none)"}`);

    if (!download_url || !user_agent) {
      console.log(`[handler] ${requestId} rejected: missing required fields`);
      return jsonResponse(
        { error: "'download_url' and 'user_agent' are required." },
        400,
      );
    }

    // Validate the download URL
    let parsedUrl;
    try {
      parsedUrl = new URL(download_url);
      if (!["http:", "https:"].includes(parsedUrl.protocol)) throw new Error();
    } catch {
      console.log(`[handler] ${requestId} rejected: invalid download_url`);
      return jsonResponse({ error: "Invalid download_url" }, 400);
    }

    // ---- Fetch the source file (streaming) ----
    console.log(`[handler] ${requestId} fetching source: ${download_url}`);
    let sourceResp;
    try {
      sourceResp = await fetch(download_url, {
        headers: { "User-Agent": user_agent },
        redirect: "follow",
      });
    } catch (err) {
      console.error(`[handler] ${requestId} download fetch error: ${err.message}`);
      console.error(`[handler] ${requestId} download fetch stack: ${err.stack}`);
      return jsonResponse({ error: `Download failed: ${err.message}` }, 502);
    }

    console.log(`[handler] ${requestId} source response: status=${sourceResp.status}, Content-Type=${sourceResp.headers.get("Content-Type")}, Content-Length=${sourceResp.headers.get("Content-Length")}`);

    if (!sourceResp.ok) {
      console.error(`[handler] ${requestId} source returned non-OK: ${sourceResp.status}`);
      return jsonResponse(
        { error: `Source returned HTTP ${sourceResp.status}` },
        502,
      );
    }

    // ---- Prepare S3 parameters ----
    const bucket = env.S3_BUCKET;
    const region = env.S3_REGION || "us-west-2";
    const endpoint = env.S3_ENDPOINT || "";
    const key = objectKeyFromUrl(download_url, key_prefix || "");
    const sourceContentType =
      sourceResp.headers.get("Content-Type") || "application/octet-stream";
    const contentLength = sourceResp.headers.get("Content-Length");

    const numericLength = contentLength ? Number(contentLength) : 0;
    const useMultipart = !numericLength || numericLength > MAX_SINGLE_PUT_SIZE;

    console.log(`[handler] ${requestId} S3 target: bucket=${bucket}, region=${region}, endpoint=${endpoint || "(default)"}, key=${key}`);
    console.log(`[handler] ${requestId} upload strategy: ${useMultipart ? `multipart (size: ${numericLength || "unknown"})` : `single PUT (${numericLength} bytes)`}`);

    const aws = makeAwsClient(env);

    // ---- Upload to S3 ----
    const uploadStart = Date.now();
    try {
      if (!useMultipart) {
        // Known size ≤ 100 MiB → single streaming PUT (zero extra memory)
        await putObjectStreaming(
          aws,
          bucket,
          region,
          key,
          sourceResp.body,
          contentLength,
          sourceContentType,
          endpoint,
        );
      } else {
        // Unknown size or > 100 MiB → multipart streaming upload (≤ 5 MiB buffer)
        await multipartStreamUpload(
          aws,
          bucket,
          region,
          key,
          sourceResp.body,
          sourceContentType,
          endpoint,
        );
      }
    } catch (err) {
      const elapsed = ((Date.now() - uploadStart) / 1000).toFixed(1);
      console.error(`[handler] ${requestId} S3 upload failed after ${elapsed}s: ${err.message}`);
      console.error(`[handler] ${requestId} S3 upload error stack: ${err.stack}`);
      return jsonResponse({ error: `S3 upload failed: ${err.message}` }, 502);
    }

    const elapsed = ((Date.now() - uploadStart) / 1000).toFixed(1);
    console.log(`[handler] ${requestId} upload completed in ${elapsed}s`);

    return jsonResponse({
      ok: true,
      bucket,
      key,
      content_type: sourceContentType,
      ...(contentLength ? { size_bytes: Number(contentLength) } : {}),
    });
  },
};
