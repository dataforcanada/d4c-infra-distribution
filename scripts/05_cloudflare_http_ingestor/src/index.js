import { AwsClient } from "aws4fetch";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB – S3 minimum for multipart parts

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

// ---------------------------------------------------------------------------
// S3 Upload – Single PUT (streaming, requires known Content-Length)
// ---------------------------------------------------------------------------

async function putObjectStreaming(aws, bucket, region, key, body, contentLength, contentType, endpoint) {
  const url = s3Url(bucket, key, region, endpoint);
  const headers = {
    "Content-Type": contentType || "application/octet-stream",
    "Content-Length": String(contentLength),
    "x-amz-acl": "bucket-owner-full-control",
  };

  const resp = await aws.fetch(url, {
    method: "PUT",
    headers,
    body, // ReadableStream – streamed directly, no buffering
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`S3 PUT failed (${resp.status}): ${text}`);
  }
  return resp;
}

// ---------------------------------------------------------------------------
// S3 Upload – Multipart (streaming, for unknown Content-Length)
// ---------------------------------------------------------------------------

async function initiateMultipart(aws, bucket, region, key, contentType, endpoint) {
  const url = `${s3Url(bucket, key, region, endpoint)}?uploads`;
  const resp = await aws.fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": contentType || "application/octet-stream",
      "x-amz-acl": "bucket-owner-full-control",
    },
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Initiate multipart failed (${resp.status}): ${text}`);
  }
  const xml = await resp.text();
  const match = xml.match(/<UploadId>(.+?)<\/UploadId>/);
  if (!match) throw new Error("Could not parse UploadId from response");
  return match[1];
}

async function uploadPart(aws, bucket, region, key, uploadId, partNumber, body, length, endpoint) {
  const url = `${s3Url(bucket, key, region, endpoint)}?partNumber=${partNumber}&uploadId=${encodeURIComponent(uploadId)}`;
  const resp = await aws.fetch(url, {
    method: "PUT",
    headers: { "Content-Length": String(length) },
    body,
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Upload part ${partNumber} failed (${resp.status}): ${text}`);
  }
  const etag = resp.headers.get("ETag");
  return etag;
}

async function completeMultipart(aws, bucket, region, key, uploadId, parts, endpoint) {
  const partsXml = parts
    .map((p) => `<Part><PartNumber>${p.partNumber}</PartNumber><ETag>${p.etag}</ETag></Part>`)
    .join("");
  const xmlBody = `<CompleteMultipartUpload>${partsXml}</CompleteMultipartUpload>`;
  const url = `${s3Url(bucket, key, region, endpoint)}?uploadId=${encodeURIComponent(uploadId)}`;
  const resp = await aws.fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/xml" },
    body: xmlBody,
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Complete multipart failed (${resp.status}): ${text}`);
  }
  return resp;
}

async function abortMultipart(aws, bucket, region, key, uploadId, endpoint) {
  const url = `${s3Url(bucket, key, region, endpoint)}?uploadId=${encodeURIComponent(uploadId)}`;
  try {
    await aws.fetch(url, { method: "DELETE" });
  } catch {
    // best-effort cleanup
  }
}

/**
 * Read from a ReadableStream in ≥ MIN_PART_SIZE chunks and upload each as an
 * S3 multipart part.  Memory usage stays bounded to ~MIN_PART_SIZE at a time.
 */
async function multipartStreamUpload(aws, bucket, region, key, stream, contentType, endpoint) {
  const uploadId = await initiateMultipart(aws, bucket, region, key, contentType, endpoint);
  const parts = [];
  let partNumber = 1;
  let buffer = [];
  let bufferSize = 0;

  const reader = stream.getReader();

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer.push(value);
      bufferSize += value.byteLength;

      // Flush when we've accumulated enough for a part
      if (bufferSize >= MIN_PART_SIZE) {
        const blob = new Blob(buffer);
        const etag = await uploadPart(aws, bucket, region, key, uploadId, partNumber, blob, bufferSize, endpoint);
        parts.push({ partNumber, etag });
        partNumber++;
        buffer = [];
        bufferSize = 0;
      }
    }

    // Upload remaining bytes as the final part
    if (bufferSize > 0) {
      const blob = new Blob(buffer);
      const etag = await uploadPart(aws, bucket, region, key, uploadId, partNumber, blob, bufferSize, endpoint);
      parts.push({ partNumber, etag });
    }

    await completeMultipart(aws, bucket, region, key, uploadId, parts, endpoint);
  } catch (err) {
    await abortMultipart(aws, bucket, region, key, uploadId, endpoint);
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

export default {
  async fetch(request, env) {
    // ---- Method check ----
    if (request.method !== "POST") {
      return textResponse("Method Not Allowed. Use POST.", 405);
    }

    // ---- Auth check ----
    const authHeader = request.headers.get("Authorization") || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
    if (!token || token !== env.AUTH_TOKEN) {
      return textResponse("Unauthorized", 401);
    }

    // ---- Content-Type check ----
    const ct = request.headers.get("Content-Type") || "";
    if (!ct.includes("application/json")) {
      return textResponse("Content-Type must be application/json", 415);
    }

    // ---- Parse body ----
    let payload;
    try {
      payload = await request.json();
    } catch {
      return textResponse("Invalid JSON body", 400);
    }

    const { download_url, user_agent, key_prefix } = payload;
    if (!download_url || !user_agent) {
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
      return jsonResponse({ error: "Invalid download_url" }, 400);
    }

    // ---- Fetch the source file (streaming) ----
    let sourceResp;
    try {
      sourceResp = await fetch(download_url, {
        headers: { "User-Agent": user_agent },
        redirect: "follow",
      });
    } catch (err) {
      return jsonResponse({ error: `Download failed: ${err.message}` }, 502);
    }

    if (!sourceResp.ok) {
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

    const aws = makeAwsClient(env);

    // ---- Upload to S3 ----
    try {
      if (contentLength && Number(contentLength) > 0) {
        // Known size → single streaming PUT (zero extra memory)
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
        // Unknown size → multipart streaming upload (≤ 5 MiB buffer)
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
      return jsonResponse({ error: `S3 upload failed: ${err.message}` }, 502);
    }

    return jsonResponse({
      ok: true,
      bucket,
      key,
      content_type: sourceContentType,
      ...(contentLength ? { size_bytes: Number(contentLength) } : {}),
    });
  },
};
