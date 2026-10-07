# Databend captures

What Databend's HTTP query API answered over `node:http` before any provider code ran, one scenario per file.
The Databend transport tests and the replay read these files; no test here reaches a live server.

## Where they come from

`tests/live/databend-evidence.ts --target local` runs every scenario of `tests/live/databend-evidence-plan.ts` against the `databend-http` fixture of `docker/databend/README.md` and writes `<target>-<date>-v<version>/<scenario>.json` for each, plus `manifest.json`.
The date is the UTC day of the run and the version is the server's `x-databend-version` header.
`manifest.json` names the target, the Studio commit the harness ran from, the harness files (`database-compose.yml`, `docker/databend/`, the plan, the scrub and the harness) that differ from that commit or are not in it, the image as `tag@digest`, the answer of `SELECT version()`, the date, and each scenario's result, time in milliseconds and number of exchanges.
A capture whose `uncommitted` list is not empty is not reproducible from its commit alone; capture again from the commit that holds those files.
A scenario whose answers do not show what the plan expects stops the run, and nothing is written.

Each scenario file holds its exchanges in order, each with the plan step that sent it (`query`, `pages`, `next`, `final`, `kill` or `logout`), the request (method, path, allow-listed headers, body) and the answer (status, allow-listed headers, body).
A body that is not JSON, such as the empty answer to a kill, is kept as `{"text": ...}`.

## The scrub

Every exchange passes through `tests/helpers/databend-evidence-scrub.ts` before it is written (C23).
Only the named request headers (`content-type`, `x-databend-client-caps`, `x-databend-session`, `x-databend-query-id`), the named answer headers (`content-type` and the `x-databend-query-*`, `x-databend-session*` and `x-databend-version` headers) and the named fields of the request and of the answer are kept; `dropped` lists the field names that were not.
`authorization`, cookies and dates never reach a file.
Query, session and node ids become `<query-N>`, `<session-N>` and `<node-N>`, the same placeholder for the same id across every file of a run, inside links, `session.internal` and the base64 `x-databend-session` header too, which is encoded again with its `=` padding so a replay can send it.
IP addresses become `<ip-N>`, user names `<user-N>` (`<user-1>` is the default user `libredb`, `<user-2>` is `studio_reader`) and a Cloud tenant `<tenant>`.
The harness writes nothing at all while any file holds a password or a `user:password`, raw, percent-encoded in either case, form-encoded or in standard or URL-safe base64 with or without padding, the host, the tenant, the warehouse, an email address or the egress IP, decoded base64 included.

## The scenarios

| Scenario | What it shows |
|---|---|
| `version` | `SELECT version()`, the source of the manifest's server version |
| `select-pages` | 25 rows over three pages with `max_rows_per_page: 10`, the `next_uri` chain to the final link |
| `every-type` | Every row of `libredb_demo.every_type` in the `display` result mode: unsafe `Int64`, `UInt64` at its maximum, `NaN` and both infinities, nested NULLs, upper-case hex `Binary`, `UInt16` and `UInt32` at their maximum, `Interval`, `Geometry` and `Geography` as GeoJSON, `Vector(2)`, `Timestamp_Tz` with its offset, `Variant`, `Array`, `Map`, `Tuple` and the `Bitmap` placeholder |
| `show-create-every-type` | The DDL the replay re-creates the every-type table from |
| `show-create-materialized-view` | `SHOW CREATE MATERIALIZED VIEW` of `libredb_demo.every_type_mv`, backticked DDL |
| `ddl` | A `CREATE TEMP TABLE` answer: `has_result_set: false` and `need_keep_alive: true`, then the logout |
| `dml` | An `INSERT` answer, one row in the column `number of rows inserted` |
| `temp-table-logout` | A temporary table that `need_keep_alive` keeps, its logout, and the 1025 Unknown table that follows in the same session |
| `begin` | `BEGIN` answering `txn_state: Active`, then `ROLLBACK` answering `AutoCommit` |
| `session-echo` | Pinned session settings echoed back, the unknown `no_such_setting` dropped from the echo |
| `error-position` | An in-body 1005 over HTTP 200 whose message carries `--> SQL:1:10` |
| `auth-401` | A wrong password: HTTP 401 with 5100 |
| `kill` | A running query, its kill (an empty 200), and the 400 its next page answers afterwards |
| `final` | A query closed by its final link after the first of three pages, and the 400 its next page answers afterwards |
| `reader` | `studio_reader` reading `libredb_demo` as `studio_ro` |

The temporary tables are the harness's only writes, and they end with their client session.
