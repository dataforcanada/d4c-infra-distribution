# cf-data-ingestor

A Cloudflare Worker that acts as a secure proxy: it downloads a file from a
URL provided in a JSON payload and streams it directly into an S3 bucket in
`us-west-2`, keeping memory usage constant regardless of file size. All uploads
use multipart upload with 5 MiB chunks to stay well within the Workers 128 MiB
memory limit.

## Architecture

```
Client POST ──▶ Worker ──stream──▶ S3 PutObject / Multipart
                  │
                  ├─ Auth check (Bearer token)
                  ├─ Fetch source URL (custom User-Agent)
                  └─ Sign with AWS Sig V4 (aws4fetch)

Client PUT  ──▶ Worker ──stream──▶ S3 PutObject / Multipart
                  │
                  ├─ Auth check (Bearer token)
                  └─ Direct binary upload (X-S3-Key header)
```

**All uploads use S3 multipart upload with 5 MiB parts**, keeping peak memory
bounded to ~5 MiB regardless of file size. This avoids hitting the Cloudflare
Workers 128 MiB memory limit that can occur when buffering large single PUT
request bodies.

## Setup

### 1. Install dependencies

```bash
pnpm install
```

### 2. Configure `wrangler.toml`

Edit the `[vars]` section:

```toml
[vars]
S3_BUCKET   = "us-west-2.opendata.source.coop"
S3_REGION   = "us-west-2"
S3_ENDPOINT = ""
```

`S3_ENDPOINT` should be left empty when targeting AWS S3 (path-style
addressing is used automatically). Set it only for non-AWS S3-compatible
services — `https://` is prepended automatically if omitted.

### 3. Set secrets

Copy the example `.env` file and fill in your values:

```bash
cp .env.example .env
```

```env
AUTH_TOKEN="your-auth-token"
AWS_ACCESS_KEY_ID="AKIAxxxxxxxxxxxxxxxxxxxx"
AWS_SECRET_ACCESS_KEY="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
```

Wrangler automatically loads the `.env` file during local development
(`pnpm run dev`). For **deployed** Workers, push each secret with:

```bash
pnpm wrangler secret put AUTH_TOKEN
pnpm wrangler secret put AWS_ACCESS_KEY_ID
pnpm wrangler secret put AWS_SECRET_ACCESS_KEY
```

### 4. Deploy

```bash
pnpm run deploy
```

## Usage

### Download mode (POST)

Downloads a file from a URL and uploads it to S3.

**Method:** `POST`
**Content-Type:** `application/json`
**Authorization:** `Bearer <AUTH_TOKEN>`

**Payload parameters:**

| Field | Required | Description |
|---|---|---|
| `download_url` | Yes | Direct link to the source file |
| `user_agent` | Yes | User-Agent string for the download request |
| `key_prefix` | No | Destination path within the S3 bucket |

#### Example

```bash
curl -X POST https://cf-data-ingestor.labs.dataforcanada.org \
  -H "Authorization: Bearer <AUTH_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "download_url": "https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Imagerie/Orthomosaique/Generique/Mosa30rvb0015_30cm_Rvb/Mtm9/Jpeg2000/mos_14_31n02_se_30cm_f09.JP2",
    "user_agent": "Data for Canada - d4c-datapkg-orthoimagery",
    "key_prefix": "dataforcanada/d4c-datapkg-orthoimagery/archive/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec"
  }'
```

#### Successful response

```json
{
    "ok": true,
    "bucket": "us-west-2.opendata.source.coop",
    "key": "dataforcanada/d4c-datapkg-orthoimagery/archive/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebecdataforcanada/.../mos_14_31n02_se_30cm_f09.JP2",
    "content_type": "application/x-msdownload",
    "size_bytes": 773722941,
    "etag": "abc123def456",
    "multipart_part_size": 5242880,
    "multipart_number_parts": 148,
    "started_at": "2026-03-12T21:00:00.000Z",
    "finished_at": "2026-03-12T21:01:30.000Z"
}
```

> `multipart_part_size` and `multipart_number_parts` are always present since all uploads use multipart.

### Direct upload mode (PUT)

Uploads a binary file body directly to S3. Useful for uploading local files
(e.g. Parquet artifacts) without needing a public download URL.

**Method:** `PUT`
**Authorization:** `Bearer <AUTH_TOKEN>`

**Required headers:**

| Header | Description |
|---|---|
| `X-S3-Key` | Full S3 object key (e.g. `dataforcanada/my-dataset/data.parquet`) |

**Optional headers:**

| Header | Description |
|---|---|
| `Content-Type` | MIME type (default: `application/octet-stream`) |
| `Content-Length` | File size in bytes |

**Body:** Raw binary file content.

#### Example

```bash
curl -X PUT https://cf-data-ingestor.labs.dataforcanada.org \
  -H "Authorization: Bearer <AUTH_TOKEN>" \
  -H "X-S3-Key: dataforcanada/my-dataset/downloads.parquet" \
  -H "Content-Type: application/octet-stream" \
  -H "Content-Length: $(stat -c%s downloads.parquet)" \
  --data-binary @downloads.parquet
```

#### Successful response

```json
{
    "ok": true,
    "bucket": "us-west-2.opendata.source.coop",
    "key": "dataforcanada/my-dataset/downloads.parquet",
    "content_type": "application/octet-stream",
    "size_bytes": 45231,
    "etag": "def456abc789",
    "started_at": "2026-03-12T21:00:00.000Z",
    "finished_at": "2026-03-12T21:00:01.000Z"
}
```

### Response fields

| Field | Type | Always present | Description |
|---|---|---|---|
| `ok` | boolean | Yes | `true` on success |
| `bucket` | string | Yes | S3 bucket name |
| `key` | string | Yes | S3 object key |
| `content_type` | string | Yes | MIME type of the uploaded file |
| `size_bytes` | number | When Content-Length known | File size in bytes |
| `etag` | string | When available | S3 ETag (quotes stripped) |
| `multipart_part_size` | number | Yes | Part size in bytes (5 MiB) |
| `multipart_number_parts` | number | Yes | Number of parts uploaded |
| `started_at` | string | Yes | ISO-8601 UTC timestamp when processing started |
| `finished_at` | string | Yes | ISO-8601 UTC timestamp when processing finished |

### Error responses

| Status | Meaning |
|--------|---------|
| 401 | Missing or invalid Bearer token |
| 405 | Non-POST/PUT method |
| 415 | Content-Type is not `application/json` (POST only) |
| 400 | Malformed JSON, missing fields, or missing `X-S3-Key` header |
| 502 | Source download or S3 upload failed |

## S3 Object Key

### POST mode

Only the **filename** is extracted from the `download_url` and placed under the `key_prefix`. The source URL's directory hierarchy is not preserved.

```
download_url: https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Imagerie/Orthomosaique/Generique/Mosa30rvb0015_30cm_Rvb/Mtm9/Jpeg2000/mos_14_31n02_se_30cm_f09.JP2
key_prefix:   "dataforcanada/d4c-datapkg-orthoimagery/archive/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec"
→ key:        dataforcanada/d4c-datapkg-orthoimagery/archive/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec/mos_14_31n02_se_30cm_f09.JP2
```

If `key_prefix` is omitted or empty, the file uploads to the bucket root.

### PUT mode

The full S3 key is specified directly via the `X-S3-Key` header.

## Local Development

```bash
pnpm run dev
```

Then POST or PUT to `http://localhost:8787`. Wrangler reads secrets from the `.env` file you created in step 3. You can also create environment-specific overrides (e.g. `.env.staging`) — see the [Cloudflare docs](https://developers.cloudflare.com/workers/configuration/secrets/#local-development-with-secrets) for the full `.env` precedence rules.
