# cf-data-ingestor

A Cloudflare Worker that acts as a secure proxy: it downloads a file from a
URL provided in a JSON payload and streams it directly into an S3 bucket in
`us-west-2`, keeping memory usage constant regardless of file size.

## Architecture

```
Client POST ──▶ Worker ──stream──▶ S3 PutObject / Multipart
                  │
                  ├─ Auth check (Bearer token)
                  ├─ Fetch source URL (custom User-Agent)
                  └─ Sign with AWS Sig V4 (aws4fetch)
```

**Two upload paths are used automatically:**

| Condition | Upload method | Memory overhead |
|---|---|---|
| Known size ≤ 100 MiB | Single streaming `PUT` | ~0 (pipe-through) |
| Unknown size **or** > 100 MiB | Multipart upload in 5 MiB chunks | ≤ 5 MiB |

> Files larger than 100 MiB always use multipart upload because Cloudflare
> Workers enforce a body-size limit on single outbound `fetch()` requests.

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

### Request

**Method:** `POST`
**Content-Type:** `application/json`
**Authorization:** `Bearer <AUTH_TOKEN>`

**Payload parameters:**

| Field | Required | Description |
|---|---|---|
| `download_url` | Yes | Direct link to the source file |
| `user_agent` | Yes | User-Agent string for the download request |
| `key_prefix` | No | Destination path within the S3 bucket |

### Example

```bash
curl -X POST https://cf-data-ingestor.<your-subdomain>.workers.dev \
  -H "Authorization: Bearer <AUTH_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "download_url": "https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Imagerie/Orthomosaique/Generique/Mosa30rvb0015_30cm_Rvb/Mtm9/Jpeg2000/mos_14_31n02_se_30cm_f09.JP2",
    "user_agent": "Data for Canada - d4c-datapkg-orthoimagery",
    "key_prefix": "dataforcanada/d4c-datapkg-orthoimagery/archive/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec"
  }'
```

### Successful response

```json
{
    "ok": true,
    "bucket": "us-west-2.opendata.source.coop",
    "key": "dataforcanada/d4c-datapkg-orthoimagery/archive/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec/mos_14_31n02_se_30cm_f09.JP2",
    "content_type": "application/x-msdownload",
    "size_bytes": 773722941
}
```

### Error responses

| Status | Meaning |
|--------|---------|
| 401 | Missing or invalid Bearer token |
| 405 | Non-POST method |
| 415 | Content-Type is not `application/json` |
| 400 | Malformed JSON or missing fields |
| 502 | Source download or S3 upload failed |

## S3 Object Key

Only the **filename** is extracted from the `download_url` and placed under the `key_prefix`. The source URL's directory hierarchy is not preserved.

```
download_url: https://diffusion.mern.gouv.qc.ca/diffusion/RGQ/Imagerie/Orthomosaique/Generique/Mosa30rvb0015_30cm_Rvb/Mtm9/Jpeg2000/mos_14_31n02_se_30cm_f09.JP2
key_prefix:   "dataforcanada/d4c-datapkg-orthoimagery/archive/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec"
→ key:        dataforcanada/d4c-datapkg-orthoimagery/archive/ca-qc_government_and_municipalities_of_quebec-2026A000224_d4c-datapkg-orthoimagery_orthorectified_imagery_from_quebec/mos_14_31n02_se_30cm_f09.JP2
```

If `key_prefix` is omitted or empty, the file uploads to the bucket root.

## Local Development

```bash
pnpm run dev
```

Then POST to `http://localhost:8787`. Wrangler reads secrets from the `.env` file you created in step 3. You can also create environment-specific overrides (e.g. `.env.staging`) — see the [Cloudflare docs](https://developers.cloudflare.com/workers/configuration/secrets/#local-development-with-secrets) for the full `.env` precedence rules.
