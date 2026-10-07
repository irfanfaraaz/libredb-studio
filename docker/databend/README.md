# Databend fixture

The live Databend server of `database-compose.yml` that the databend provider talks to over Databend's own HTTP API, and the fixture that seeds it.
The provider's captures (`tests/fixtures/databend/`), the evidence harness and the live check run against this server.
The compat `databend` service of the same file is the mysql provider's MySQL-wire probe; it is a different service and is left alone.

## The server

| Service | Profile | From the host | Authentication |
|---|---|---|---|
| `databend-http` | none | HTTP `127.0.0.1:8000` | Basic, user `libredb`, password `Probe123pass!` |
| `databend-http-seed` | none | nothing | One-shot, the same user |

The server runs `datafuselabs/databend:v1.2.951-nightly`, pinned by the digest `sha256:f63585cae3e096d62580ad51d92abd2f64b57b196af3b51cb01ecae381ec874b`; `SELECT version()` answers `v1.2.951-nightly-9b7eeff9a8`.
The password is the compat service's spelling, a fixed test value.
The port is bound to the loopback address only.
Tables live on the container's file system (`QUERY_STORAGE_TYPE: fs`) and there is no data volume: removing the container resets the server.
Databend writes its query log to `/var/log/databend/databend-query-default.*` inside the container, not to `docker logs`, which carries only the startup banner; a check of what the server logged reads that file.

## Bounds, as measured

Measured on 2026-10-08 on this image, with the fixture seeded:

- Start: the container answered `/health` 2.2 s after it started (the first probe at 1.1 s failed to connect, the second passed), so `start_period: 30s` leaves more than ten times that.
- Resident size: 110.8 MiB idle after the seed (`docker stats --no-stream libredb-databend-http`), and a cgroup peak (`memory.peak`) of 160 MiB after a sort over 20 million generated rows and one page of a million rows.
- The bound is `memory: 1G` and 2 CPUs, about six times the measured peak, with no swap past it.
- The seed one-shot is bounded at 128 MiB and half a CPU; it runs only `sh` and `curl`.

## Bringing it up

Name the services: an unnamed `up` starts every engine of the file.

```sh
docker compose -f database-compose.yml up -d databend-http databend-http-seed
curl -fsS http://127.0.0.1:8000/health
DATABEND_PASSWORD='Probe123pass!'
curl -sS -u "libredb:$DATABEND_PASSWORD" -H 'content-type: application/json' -d '{"sql":"SELECT version()"}' http://127.0.0.1:8000/v1/query
docker compose -f database-compose.yml ps -a databend-http-seed
```

The seed waits for a healthy server, prints `seed.sh: <n> statements` and exits 0; any status other than 200, an in-body error or a `Failed` state stops it non-zero with the server's answer.
It may run again (`docker compose -f database-compose.yml up -d databend-http-seed`): every table and view is replaced, the role is created only when missing and the user is replaced, so a second run leaves the same fixture.
Remove the two containers by name, never with `down`:

```sh
docker compose -f database-compose.yml rm -sf databend-http databend-http-seed
```

## The fixture

`fixture.jsonl` holds one statement per line as `{"sql": "..."}`; `seed.sh` posts each in order to `/v1/query` and follows `next_uri` until the statement ends.

| Object | What it is for |
|---|---|
| `libredb_demo.every_type` | One column of every type of design section 4: the signed integers, `UInt8` to `UInt64` with `UInt16`, `UInt32` and `UInt64` at their maximum, `Int64` past 2^53 and at both ends, `Float32` and `Float64` with `NaN` and both infinities, `Decimal(38, 10)`, `Boolean`, `String` with a quote, a backslash and a double quote, `Binary` whose hex has letters, `Date`, `Timestamp`, `Timestamp_Tz` with three offsets, `Interval`, `Geometry`, `Geography`, `Variant`, `Array`, `Map` and `Tuple` holding nested NULLs, `Vector(2)` and `Bitmap`; row 4 is NULL in every nullable column |
| `libredb_demo.every_type_view` | A view over three of its columns |
| `libredb_demo.every_type_mv` | A materialized view over two of its columns; the pinned image creates one without a license |
| `libredb_demo.wide_60` | A table of 60 `INT` columns and one row |
| `studio_demo.notes` | The table the write scenarios of the live check change |
| role `studio_ro` | `SELECT` on `libredb_demo.*`, nothing else |
| user `studio_reader` | Password `Reader123pass!`, a fixed test value; holds `studio_ro` as its default role and no other grant (the `public` role every Databend user has aside), the least-privilege user agent plan mode needs, since it refuses a superuser |

The only writers of this server are `seed.sh` and `tests/live/databend-live-check.ts`, which writes only to `studio_demo` and `libredb_demo`; `tests/unit/db/databend/live-environment.test.ts` holds both rules, and the evidence harness creates nothing but temporary tables that end with their session.
