# Can the Rust SQL node run sysbench?

**Verdict: not yet — and the reason is not the SQL.**

Every statement `sysbench`'s `oltp_*` workloads issue is accepted and answered
correctly, with results byte-identical to a real Go `tidb-server` reading the
same TiKV. What blocks a sysbench *number* is three things around the SQL: the
client cannot complete the connection, `CREATE INDEX` is refused, and an
`AUTO_INCREMENT` table becomes invisible to the node that just created it.

Measured on 2026-07-30, `origin/hparser-integration` at `9569d280dd`, release
build, macOS arm64, against a `tiup playground v8.5.6` cluster (1 PD, 1 TiKV,
1 Go `tidb-server`) with the Rust node in `--cluster-session` mode.

Reproduce with `rust/scripts/run-sysbench-ladder.sh` (starts and tears down
everything it uses under an EXIT/INT/TERM trap that fails the run if any owned
port is still reachable).

## What the ladder measured, rung by rung

| Rung | Result |
| --- | --- |
| 0. TiUP playground (PD + TiKV + Go TiDB) | OK |
| 1. Rust node startup, `--cluster-session` | OK, readiness event `cluster_session_node_ready` |
| 2. Stock MySQL client handshake, auth, `SELECT 1` | OK, `mysql_native_password` |
| 3. `CREATE DATABASE sbtest` through the Rust node | OK |
| 4. `sysbench oltp_read_only ... prepare` | **FAIL — cannot connect (blocker 1)** |
| 5. Dataset correctness | skipped, no dataset |
| 6. `oltp_point_select` / `read_only` / `write_only` / `read_write`, both `--db-ps-mode=disable` and default | **all FAIL identically at connect** |
| 7. sysbench's own statements driven by hand | 17 accepted, 3 refused |

No throughput figure is reported, because none was produced. A QPS number is
not available at any `--db-ps-mode`: text and binary prepared-statement paths
fail at the same place, before either is exercised.

## Blocker 1 (architectural): the node never advertises `CLIENT_SSL`

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

The initial-handshake capability flags name the difference exactly:

```
rust: capabilities=0x00158208 CLIENT_SSL=no
go:   capabilities=0x0015aeaf CLIENT_SSL=yes
```

A control run of the same sysbench binary against the Go `tidb-server` on the
same cluster connects and gets as far as `CREATE INDEX`, so the client is fine;
bit 11 of our advertised capabilities is the whole difference.

This is **not** a one-bit fix. `crates/tidb-server/src/handshake.rs` already
parses a 32-byte `SSLRequest` and exposes `tls_established()`, but the TLS
handshake itself is explicitly left to "the transport owner", and no listener
performs one: `secure_transport.rs` owns only the `RequireSecureTransport`
admission decision and states it "does not perform a TLS handshake, inspect
certificates, or authenticate". There is no server-certificate option for the
MySQL port at all — `--cluster-ssl-ca/cert/key` configure the *PD client*
connection, not inbound MySQL connections.

Advertising `CLIENT_SSL` without implementing the upgrade would be strictly
worse than the current refusal: the client would send `SSLRequest` and then
block forever on a TLS handshake that never arrives. Getting sysbench to
connect requires real inbound MySQL-port TLS (cert/key config, a rustls
acceptor wired into the connection loop). Refused as out of scope here.

There is no client-side workaround on this machine: sysbench exposes only a
boolean `--mysql-ssl`, has no `ssl-mode` knob, and Connector/C does not read
option files unless the application asks it to, which sysbench does not.

## Blocker 2: `CREATE INDEX` is refused

`oltp_common.lua:238` runs `CREATE INDEX k_1 ON sbtest1(k)` during `prepare`.
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

## Blocker 3: an `AUTO_INCREMENT` table is created and then not served

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

## Shortest path to a real sysbench number

1. Inbound MySQL-port TLS, so `CLIENT_SSL` can be advertised honestly. This is
   the only hard blocker — nothing else can be worked around by sysbench flags.
   (Alternatively, a sysbench built against libmysqlclient rather than
   Connector/C 3.4 would connect today; the one on this machine is not.)
2. `CREATE INDEX` in the DDL surface, so `prepare` runs unmodified.
3. Resolve the `AUTO_INCREMENT` create/serve mismatch, or accept
   `--auto-inc=off` as the documented configuration.

With those, rungs 4 through 6 should run as written, and rung 5's Rust-vs-Go
checksum comparison is already in place to keep any resulting number honest.

## Unrelated environment note

The Go control rung also failed, but on the Go side and for a local reason:
`CREATE INDEX` returned `error 8256: Check ingest environment failed: no enough
space in /tmp/tidb/tmp_ddl-45000`. That is this machine's disk, not a TiDB or
Rust defect. A full sysbench run against the Go server as a baseline will need
space freed for the playground's DDL temp directory.
