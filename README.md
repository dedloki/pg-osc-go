# pg-osc (Go)

Go port of [pg-osc](https://github.com/shayonj/pg-osc) (`pg-online-schema-change`) with the `--filter` / `--wait-for-swap` / batched copy additions of [dedloki/pg-osc](https://github.com/dedloki/pg-osc) and [dedloki/pg-osc-rust](https://github.com/dedloki/pg-osc-rust). Same options, same behaviour, same JSON logs as the Rust version. One fully static binary (musl, no glibc or OpenSSL dependency), no Ruby.

It applies `ALTER TABLE` to a busy table through a shadow table: a trigger records changes into an audit table, the data is copied into the altered shadow table, the audit table is replayed, and the tables are swapped under a short `ACCESS EXCLUSIVE` lock. See the upstream README for the details and caveats of the approach.

## Install on Debian 12

```
sudo dpkg -i pg-osc-go_<version>-1_amd64.deb      # or arm64
pg-online-schema-change version
```

or just copy `pg-online-schema-change-linux-<arch>` to the host. The package conflicts with the Rust `pg-osc` package (same binary path).

## Usage

```
export PGPASSWORD=...
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE' \
  --dbname postgres --host localhost --username jamesbond
```

`pg-online-schema-change perform --help` lists every option. They are the same as in the Ruby version, except:

- `-w` is `--wait-time-for-lock` (the Ruby CLI had it twice); the password goes into `PGPASSWORD` (or `--password`).
- `--sslmode disable|prefer|require` (or `PGSSLMODE`), default `prefer`. As in libpq the certificate is not verified.
- `--help` has no short form, `-h` is the host.
- exit code after `SIGTERM`/`SIGINT`/`SIGQUIT` is 128 + signal, 2 for command line errors.

### Copy a subset of rows and swap on demand

```
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE books ALTER COLUMN email SET NOT NULL' \
  --filter 'email IS NOT NULL' \
  --wait-for-swap \
  --dbname postgres --host localhost --username jamesbond
```

- `--filter` copies only the rows matching the condition and applies it during the replay too: an `UPDATE` that makes a row match adds it to the shadow table, one that makes it stop matching removes it. Use the original column names. Rows that don't match stay only in the old table `pgosc_op_table_<table>`.
- `--wait-for-swap` keeps the shadow table in sync after the copy (autovacuum is enabled on the audit and shadow tables meanwhile) until a swap is requested:

  ```
  pg-online-schema-change swap --table books --schema public --dbname postgres --host localhost --username jamesbond
  ```

  or `kill -USR1 <pid>` (the pid is in the logs). If the swap can't get its lock, the sync goes on and waits for the next request. A stop signal removes the trigger, audit and shadow tables, see below.
- On failure the old table is never dropped, even with `--drop`.
- Foreign keys from other tables pointing to filtered-out rows fail `VALIDATE CONSTRAINT` after the swap (the swap is done by then, the constraints stay `NOT VALID`). Fix those rows first or use `--skip-foreign-key-validation`.
- Before the swap the shadow table lags the source by the not yet replayed audit rows, so `count(*)` differs both ways. They are equal right after the swap.

### Stopping and cleanup

`SIGTERM`, `SIGINT` (Ctrl-C), `SIGQUIT` and `SIGHUP` (closed ssh session, terminal or tmux window) stop the run and clean up: the audit trigger is dropped first, then the audit and shadow tables. `DROP TRIGGER` needs an `ACCESS EXCLUSIVE` lock on the table, so it is tried with `lock_timeout = --wait-time-for-lock` and retried until it gets through (each attempt is logged); writes to the table wait at most that long behind it, and keep going to the audit table meanwhile. A second signal exits at once and logs `cleanup_sql`, the statements to finish by hand. Drop the trigger before the audit table: while the trigger is there, dropping the audit table breaks every write to the table. `kill -9` leaves everything in place, clean up with the same statements:

```
SET lock_timeout = '5s';
DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON public.<table>;  -- repeat until it succeeds
RESET lock_timeout;
DROP TABLE IF EXISTS public.pgosc_at_<table>_<id>;
DROP TABLE IF EXISTS public.pgosc_st_<table>_<id>;
```

### Big tables: batched copy and deferred indexes

```
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE barcode ALTER COLUMN stock_id SET NOT NULL' \
  --filter 'stock_id IS NOT NULL' --wait-for-swap \
  --copy-batch-pages 10000 --defer-indexes --maintenance-work-mem 4GB --skip-vacuum \
  --dbname mydb --host db.host --username myuser
```

- `--copy-batch-pages N` (PostgreSQL 14+) copies by ctid ranges of N heap pages (8kB each), every batch in its own short transaction, so the primary key type doesn't matter (uuid is fine). Without one snapshot a row changed during the copy can be copied twice or missed; every such row was changed after the trigger was set up, so it is in the audit table. After the copy the shadow rows whose primary key is in the audit table are deleted and the replay of the whole audit table brings them back in their final state. Progress is logged every minute with pages done and an ETA.
- `--defer-indexes` drops the shadow table's indexes, primary key and unique/exclusion constraints after the `ALTER` and builds them after the copy (primary key first), one sorted build per index instead of a random insert per row. Watch it in `pg_stat_progress_create_index`. `--maintenance-work-mem` is set for those builds; parallel workers follow `max_parallel_maintenance_workers`.
- Both work with `--filter`, together or apart. `--copy-batch-pages` can't be combined with `--copy-statement`.
- `--skip-vacuum` skips the `VACUUM` of the table that pg-osc runs before starting (kept from upstream as "house keeping"). The copy doesn't depend on it.
- `--verbose` logs only the first 2000 characters of each query (replay batches are thousands of statements).

## Differences from the Ruby version

Same as the Rust port:

- The replay copies rows with `INSERT ... SELECT` from the audit table instead of re-sending values as text literals: `UPDATE ... SET col = NULL` is replayed (the Ruby version lost such updates), types never round-trip through text. An `UPDATE` is replayed as delete + insert by primary key.
- Views are looked up by the old table's oid, not by name in every schema.
- After the swap the audit trigger is dropped from the old table too (the Ruby version left it there, so any write to the old table failed).
- `--copy-statement`: only `%{shadow_table}` is substituted.

Known limitation kept from upstream: a view defined as `SELECT *` over a dropped column can't be recreated after the swap (`CREATE OR REPLACE VIEW` can't drop a column). Recreate such views yourself.

## Layout

- `cmd/pg-online-schema-change` — CLI (cobra) and signal handling.
- `internal/pgosc` — the operation: `orchestrate.go` (steps, copy, sync, swap), `replay.go`, `query.go` (catalog queries), `sql.go` (libpg_query via pg_query_go: ALTER parsing/rewriting, filter validation), `db.go` (pgconn, simple protocol, the Ruby transaction semantics), `log.go`.

## Build

Static binaries for linux amd64/arm64 and `.deb` packages, cross-compiled from macOS or Linux with zig as the C compiler (pg_query_go is cgo):

```
scripts/build-debian12.sh            # into dist/
```

Needs Go 1.24+, `zig` and `nfpm` (`go install github.com/goreleaser/nfpm/v2/cmd/nfpm@latest`). For the host itself `go build ./cmd/pg-online-schema-change` is enough.

## Tests

```
docker compose up -d        # Postgres on localhost:5432, user jamesbond
go test ./...
```

Unit tests cover the SQL parsing; integration tests (`internal/pgosc/integration_test.go`) run whole operations against Postgres, each in its own schema: add column with FKs, triggers, views and storage parameters; rename + drop column with NULL updates; `--filter` with rows moving in and out; `--wait-for-swap` with a failed lock attempt; termination cleanup; the old table kept on failure; `--copy-statement`; batched copy and deferred indexes under concurrent writes on a uuid-keyed table; the binary's CLI and setup error reporting.
