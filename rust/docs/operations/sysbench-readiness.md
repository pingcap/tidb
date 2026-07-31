# Can the Rust SQL node run sysbench?

**Verdict: yes, with `--auto-inc=off --create-secondary=off`. Stock sysbench
connects, loads a table, and runs all four `oltp_*` workloads against this
node.**

The connection blocker is gone: the node now serves inbound TLS on the MySQL
port, so its capabilities read `CLIENT_SSL=yes` and MariaDB Connector/C stops
refusing it. `prepare` gets past the connect, past `CREATE TABLE`, and through
all 1,000 inserts; the first statement it still cannot get is `CREATE INDEX`.

What remains is a DDL surface question, not a connectivity one: `CREATE INDEX`
is refused (blocker 2) and `AUTO_INCREMENT` is refused at `CREATE TABLE`
(blocker 3). Both are named below and neither prevents a number.

Measured on 2026-08-01, `hparser-integration` + this unit, release build,
macOS arm64, against a `tiup playground v8.5.6` cluster (1 PD, 1 TiKV, 1 Go
`tidb-server`) with the Rust node in `--cluster-session` mode. The 2026-07-30
measurement of the same ladder, before inbound TLS, failed at rung 4 for a
different reason (the client could not connect at all).

Reproduce with `rust/scripts/run-sysbench-ladder.sh` (starts and tears down
everything it uses under an EXIT/INT/TERM trap that fails the run if any owned
port is still reachable).

## What the ladder measured, rung by rung

| Rung | Result |
| --- | --- |
| 0. TiUP playground (PD + TiKV + Go TiDB) | OK |
| 1. Rust node startup, `--cluster-session` | OK, readiness event `cluster_session_node_ready` |
| 2. Stock MySQL client handshake, auth, `SELECT 1` | OK, `mysql_native_password` |
| 2b. TLS accept control | OK both ways: `--ssl-mode=DISABLED` and `--ssl-mode=REQUIRED` each return `SELECT 1` |
| 3. `CREATE DATABASE sbtest` through the Rust node | OK |
| 3b. Capability probe | `rust: 0x00158a08 CLIENT_SSL=yes`, `go: 0x0015aeaf CLIENT_SSL=yes` |
| 4. `sysbench oltp_read_only ... prepare` | `--auto-inc=on` FAILs at `CREATE TABLE` (blocker 3); `--auto-inc=off` connects, creates the table, inserts all 1,000 rows, then **FAILs at `CREATE INDEX` (blocker 2)** |
| 5. Dataset correctness | skipped — the ladder only checks a dataset it saw `prepare` finish |
| 6. `oltp_point_select` / `read_only` / `write_only` / `read_write`, both `--db-ps-mode=disable` and default | **all eight ran** against the table rung 4 left behind |
| 7. sysbench's own statements driven by hand | 16 accepted, 4 refused (the 3 `AUTO_INCREMENT` cases and `CREATE INDEX`) |

### Throughput, `--threads=1 --time=10`, no secondary index

| Workload | text (`--db-ps-mode=disable`) | binary prepared (default) |
| --- | --- | --- |
| `oltp_point_select` | 3,931.92 qps | 3,875.83 qps |
| `oltp_read_only` | 1,292.04 qps (80.75 tps) | 1,035.92 qps (64.74 tps) |
| `oltp_write_only` | 292.16 qps (48.69 tps) | 575.10 qps (95.85 tps) |
| `oltp_read_write` | 927.41 qps (46.37 tps) | 847.49 qps (42.37 tps) |

Read these as "the workloads run", not as a benchmark result. One thread, a
1,000-row table, a laptop also running the cluster it queries, and — the part
that matters for comparison — **no secondary index**, because `CREATE INDEX`
is still refused. `oltp_read_only`'s range and `SUM(k)` queries therefore scan
where a real sysbench run would seek, so these numbers are not comparable to a
published figure or to the Go server until blocker 2 lands.

Rung 5's Rust-vs-Go checksum comparison did not run: the ladder gates it on a
`prepare` that returned success, and this one ended on `CREATE INDEX`. The
dataset was still correct enough for eight workloads to run against it, but
the independent Go-side check is the evidence that matters and it is pending.

## Blocker 1 (FIXED): inbound TLS on the MySQL port

Homebrew's `sysbench 1.0.20` links **MariaDB Connector/C 3.4.9**, not
libmysqlclient:

```
$ otool -L /opt/homebrew/bin/sysbench
	/opt/homebrew/opt/mariadb-connector-c/lib/libmariadb.3.dylib
```

Connector/C 3.4 enables TLS by default, and it refuses a server that does not
offer it — even with `--mysql-ssl=off`, which is already sysbench's default:

```
FATAL: error 2026: TLS/SSL error: SSL is required, but the server does not support it
```

The capability flags now match on the bit that mattered:

```
rust: capabilities=0x00158a08 CLIENT_SSL=yes
go:   capabilities=0x0015aeaf CLIENT_SSL=yes
```

`0x00158208` -> `0x00158a08` is exactly bit 11.

Advertising the bit alone would have been strictly worse than the old
refusal — the client would send `SSLRequest` and block forever on a handshake
that never arrives — so the upgrade is what landed, in
`crates/tidb-server/src/mysql_tls.rs`, following `pkg/server`:

- `pkg/server/server.go` sets `s.capability |= mysql.ClientSSL` **only** when
  `LoadTLSCertificates` returned a config. Here, holding an `MysqlServerTls`
  is the only way to reach the bit, so "advertised but not served" is not a
  representable state.
- `pkg/server/conn.go`'s `handshake` parses the response header first and, on
  `CLIENT_SSL`, calls `upgradeToTLS` and reads a **second** response packet.
  The client sends a truncated `SSLRequest` in the clear and then repeats a
  full `HandshakeResponse41` over the encrypted stream; sequence numbering is
  continuous across the upgrade, so every later reply sequence shifts by one.
- Certificate material comes from `--ssl-cert`/`--ssl-key`, or from a
  self-signed pair generated at startup, which is Go's `auto-tls`. Note
  `--cluster-ssl-ca/cert/key` remain a different thing entirely: they
  configure the *PD client* transport, not inbound MySQL connections.

`--auto-tls` defaults to **on** here, unlike `pkg/config`'s own `false`. The
TiUP playground Go server this node is measured against runs with auto-TLS
enabled, which is precisely why it advertised `CLIENT_SSL` and this node did
not. `--no-auto-tls` restores a plaintext-only port.

The port is not TLS-only: rung 2b checks both directions, and the stock MySQL
client connects under `--ssl-mode=DISABLED` and under `--ssl-mode=REQUIRED`.
Client-certificate authentication (`ssl-ca`, `REQUIRE X509`) is still not
served.

## Blocker 2 (now the first failure): `CREATE INDEX` is refused

This is where `prepare` stops today. `oltp_common.lua:238` runs
`CREATE INDEX k_1 ON sbtest1(k)` during `prepare`, after the table and all
1,000 rows are already in.
The node answers:

```
ERROR 1105 (HY000): this node changes the cluster's catalog for CREATE TABLE,
DROP TABLE, CREATE DATABASE and DROP DATABASE only; run this statement on a
TiDB server
```

sysbench can be told to skip it (`--create-secondary=off`), but then
`oltp_read_only`'s range and `SUM(k)` queries lose the index they exist to
exercise, and the benchmark stops measuring what it is for. Note the node does
maintain secondary indexes it was configured with (`--read-table`'s trailing
index section), so this is a DDL-surface gap, not a storage gap.

## Blocker 3 (FIXED as a refusal): `AUTO_INCREMENT` is now refused at CREATE

**Update.** The create/serve mismatch below is gone: `CREATE TABLE` and the
catalog loader now read one predicate
(`tidb_exec::cluster_auto_increment::auto_increment_refusal`), so the node no
longer writes a table it cannot serve. `CREATE TABLE ... AUTO_INCREMENT`
answers Go's own errno instead, before any mutation:

```
ERROR 8200 (HY000): Unsupported CREATE TABLE `sbtest`.`sbtest1`: its column id
is AUTO_INCREMENT, whose ids come from the cluster's own autoid allocator,
which this node does not consume
```

This converts a silent unusable table into an honest refusal. It does **not**
make sysbench's default schema work — `--auto-inc=off` is still required, and
the rung-7 result below is unchanged. Serving `AUTO_INCREMENT` needs the
allocator to get the separate-key home Go gives it (`pkg/meta/meta.go`'s
`TID:`/`IID:` keys, `pkg/meta/autoid/autoid.go`'s reserve-in-its-own-txn), a
unit of its own that must prove the counter survives a node restart.

The original report follows.

### Original report: an `AUTO_INCREMENT` table is created and then not served

sysbench's default `id INTEGER NOT NULL AUTO_INCREMENT` is accepted by
`CREATE TABLE` — and the resulting table is then invisible:

```
OK   create-table-auto-inc
FAIL auto-inc-insert-without-id: ERROR 1105 (HY000): table not found in catalog
FAIL auto-inc-select:            ERROR 1105 (HY000): table not found in catalog
```

The catalog loader skips such tables by design, reporting at startup that a
column "is AUTO_INCREMENT, whose ids come from the cluster's own autoid
allocator, which this node does not consume". The wart is that DDL admits a
shape the catalog then rejects, so the node writes a table it cannot serve.
Worth fixing independently of sysbench: either refuse the `CREATE`, or consume
the allocator.

`--auto-inc=off` avoids it — sysbench then declares `id INTEGER NOT NULL` and
supplies every id explicitly — which is how rung 7 proceeded.

## What does work: the whole sysbench statement set, and it is correct

Driven by hand through the stock MySQL client against a 1,000-row `sbtest1`
(ids supplied explicitly, no secondary index), every statement form from
`oltp_common.lua` was accepted:

- `CREATE TABLE sbtest1(id INTEGER NOT NULL, k INTEGER DEFAULT '0' NOT NULL,
  c CHAR(120) DEFAULT '' NOT NULL, pad CHAR(60) DEFAULT '' NOT NULL,
  PRIMARY KEY (id))`
- the 1,000-row bulk `INSERT ... VALUES (...),(...),...`
- `SELECT c ... WHERE id=?` (point select)
- `SELECT c ... WHERE id BETWEEN ? AND ?` (simple range)
- `SELECT SUM(k) ... WHERE id BETWEEN ? AND ?`
- `SELECT c ... BETWEEN ... ORDER BY c`
- `SELECT DISTINCT c ... BETWEEN ... ORDER BY c`
- `UPDATE ... SET k=k+1 WHERE id=?` (index update)
- `UPDATE ... SET c=? WHERE id=?` (non-index update)
- `DELETE FROM ... WHERE id=?` and the matching re-`INSERT`
- `BEGIN` / `SELECT` / `UPDATE` / `COMMIT` as one transaction

**The data is right.** The decisive check is not our own arithmetic but an
independent Go `tidb-server` reading the same TiKV rows. The two agree exactly,
before and after the write workload:

| Query | Rust node | Go TiDB |
| --- | --- | --- |
| `COUNT(*), SUM(id), SUM(k), MIN(id), MAX(id)` after load | `1000 500500 506087 1 1000` | `1000 500500 506087 1 1000` |
| `COUNT(*), SUM(id), SUM(k)` after the write/txn rungs | `1000 500500 505171` | `1000 500500 505171` |

Point, range, `SUM`, `ORDER BY` and `DISTINCT` results were all the expected
rows (`c-100` … `c-109`, `c-500`), so no rows went silently missing. Known bug
#58 (`WHERE int_pk = 1.0` returning no row for an integer/decimal comparison)
was not triggered: sysbench binds integers, and every predicate here is
integer-to-integer.

## Shortest path to an unqualified sysbench number

1. ~~Inbound MySQL-port TLS~~ — done; `CLIENT_SSL` is advertised because it is
   served.
2. `CREATE INDEX` in the DDL surface, so `prepare` runs to completion. This is
   what currently makes the rung-6 numbers non-comparable: without the
   secondary index, `oltp_read_only` scans where it should seek. It is also
   what gates rung 5's Rust-vs-Go checksum comparison, since the ladder only
   checks a dataset whose `prepare` succeeded.
3. `--auto-inc=off` remains the documented configuration; serving the clause
   needs the persistent allocator unit described under blocker 3.

With `--create-secondary=off` a number is available today, and the table above
is it.

## Unrelated environment note

The Go control rung fails, but on the Go side and for a local reason:
`CREATE INDEX` returns `error 8256: Check ingest environment failed: no enough
space in /tmp/tidb/tmp_ddl-<port>`. That is this machine's disk, not a TiDB or
Rust defect — it reproduced again on 2026-08-01. A Go-side baseline to compare
the rung-6 numbers against will need space freed for the playground's DDL temp
directory.
