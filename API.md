# Local Unix-socket API

`rust-polar serve` is the sole owner of `health.redb`. Other local programs
should use the HTTP/1.1 API through `DATA_DIR/run/rust-polar.sock`; the socket is
mode `0600` and is not exposed on TCP. Each request is bounded, and paginated
calls use a keyset `next_cursor`. Pages can observe concurrent writes between
requests, so a multi-page export is not one database-wide snapshot.

## Read endpoints

| Endpoint | Purpose |
|---|---|
| `GET /v1/status` | Data paths, archive and record counts, Polar authorization state |
| `GET /v1/health` | Schema version and record/measurement counts |
| `GET /v1/sources` | Sources present in stored record keys |
| `GET /v1/sync/status` | Scheduler state |
| `GET /v1/metrics?limit=100&cursor=...` | Metric/unit catalog and sources |
| `GET /v1/records?source=polar&kind=exercise&from=...&to=...&limit=100&cursor=...` | Provider record pages |
| `GET /v1/records/{percent-encoded-id}` | One stored provider record |
| `GET /v1/daily?from=2026-10-01&to=2026-11-01&limit=100&cursor=...` | Records selected by provider-local date; date range is `[from,to)` |
| `GET /v1/series?user_id=...&source=polar&metric=...&from=...&to=...&limit=100&cursor=...&context=...` | UTC-resolved measurements; optional `downsample_buckets=1..250` selects first/min/max/last points across the full range |
| `GET /v1/export?user_id=...&source=polar&metric=...&from=...&to=...&format=jsonl|csv&limit=1000&cursor=...` | One JSONL or CSV measurement page |
| `GET /v1/exercise-export?exercise_id=...&format=fit|tcx|gpx` | Stream a verified archived binary export |

UTC bounds use RFC3339 values with an explicit offset and are half-open `[from,to)`.
Provider-local dates are not converted to UTC. Measurements without a reliable
UTC instant stay in their parsed provider record and do not appear in `/series`.
Series values include `schema_version`, stable `id`, `provenance`, `metric`,
decimal-string `value`, `unit`, `time`, `aggregation`, `context`, and optional
`parent_id`. Export responses provide the continuation cursor in
`x-next-cursor`.

For chart-sized responses, add `downsample_buckets=N` to `/v1/series` (1–250).
The requested half-open range is split into N equal time buckets. The API scans
the range and returns the actual first, minimum, maximum, and last measurement
in each non-empty bucket, ordered by event time. Each row contains the bucket
index/start, a `selection` label, and the complete original `measurement`, so
its value, unit, time, context, and provenance remain intact. The first and last
points are selected by event time; numeric extrema are compared from the exact
decimal values. A point can appear more than once if it fills multiple roles.
This only changes the response; stored measurements are untouched. Downsampled
requests cover the whole range and cannot use `cursor`; the scan is capped at
1,000,000 points and the response at 5 MiB. Narrow the time range if the scan
bound is exceeded; request fewer buckets if the encoded response exceeds 5 MiB.

Exercise exports are returned as `application/octet-stream` with a safe
attachment filename and `Content-Length`. The API resolves the indexed
exercise/format record, verifies its archive receipt, then decompresses and
streams the object through a two-chunk queue; the response does not load the
full FIT/TCX/GPX file into memory. Unknown formats return 400, and a missing
exercise/format index returns 404.

Example:

```text
GET /v1/series?user_id=123&source=polar&metric=polar.activities.steps&from=2026-10-01T00%3A00%3A00Z&to=2026-10-08T00%3A00%3A00Z&limit=100
```

## Local administration

These `POST` endpoints are available only through the private socket. They
accept an empty body and parameters in the query string:

```text
POST /v1/admin/backup?destination=%2Fmnt%2Fbackup%2Fpolar-2026-10-21
POST /v1/admin/replay
POST /v1/admin/replay?resource=activities
POST /v1/admin/sync
POST /v1/admin/sync?from=2026-10-01&to=2026-10-21
POST /v1/admin/archive/verify
POST /v1/admin/archive/diagnose?older_than_seconds=86400
POST /v1/admin/archive/cleanup?older_than_seconds=86400&apply=false
```

`GET /v1/status` returns database/archive paths and counts without opening a
second redb handle. The archive maintenance endpoints run through the daemon
and return HTTP 409 while sync is publishing data. Diagnose and cleanup accept
`older_than_seconds` from 0 through 31536000 (default 86400). Cleanup is a dry
run unless `apply=true`; `rust-polar archive cleanup --apply` explicitly
requests deletion of the candidate set calculated in that operation.

Backup refuses an existing destination, creates a consistent redb snapshot and
copies the associated immutable response archive. OAuth credentials are
excluded; after restore, authorize Polar again. Restore is an offline operation
into a new `--data-dir`; it verifies the backup manifest and archived objects.
Do not copy a live `health.redb` file with `cp`.

`rust-polar status`, `query`, `archive verify`, `archive diagnose`,
`archive cleanup`, `archive replay`, and `backup` use the running service over
the socket when it is available. Restore still requires the service stopped.
A second process must never open the live redb database concurrently.

The daemon serializes manual and scheduled sync cycles. A manual request made
while a cycle is already running receives HTTP 409. Explicit sync dates are
inclusive and limited to 365 days for activity history; narrower resource
history windows still apply.

The CLI exports a single metric as bounded JSON Lines or CSV pages. UTC input
must include an explicit offset; ranges are half-open. The user id defaults to
the authorized Polar user id, or `polar-local` for fixture/import-only stores.

```sh
rust-polar export --source polar --metric calories \
  --from 2026-10-01T00:00:00Z --to 2026-11-01T00:00:00Z --format csv
```
