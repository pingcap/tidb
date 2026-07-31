# Can the Rust SQL node run sysbench?

**Verdict: yes, with `--auto-inc=off --create-secondary=off`. Stock sysbench
connects, loads a table, and runs all four `oltp_*` workloads against this
node.**

Every statement `sysbench`'s `oltp_*` workloads issue is accepted and answered
correctly, with results byte-identical to a real Go `tidb-server` reading the
same TiKV. All three of the blockers that stood around that SQL are now fixed
on this branch: the node serves inbound TLS on the MySQL port, so its
capabilities read `CLIENT_SSL=yes` and MariaDB Connector/C stops refusing it
(blocker 1); `CREATE INDEX` and `DROP INDEX` are catalog changes this node
performs, backfill included, verified by Go's own `ADMIN CHECK TABLE`
(blocker 2); and `AUTO_INCREMENT` allocates from the cluster's own counter
(blocker 3).

**No ladder run has been taken on a tree carrying all three fixes.** The three
were built in parallel and each ladder run below predates the others' landing,
so every rung result was measured with exactly ONE of the three present. Their
being on one branch is not a combined measurement, and this document does not
claim one: read each row as evidence about its own fix, not as the current
end-to-end state, until a run on the merged tip replaces it.

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
| 3b. Capability probe | `rust: 0x00158a08 CLIENT_SSL=yes`, `go: 0x0015aeaf CLIENT_SSL=yes` (TLS run) |
| 4. `sysbench oltp_read_only ... prepare` | TLS run: `--auto-inc=off` connects, creates the table, inserts all 1,000 rows, then FAILs at `CREATE INDEX`. Index run (pre-TLS): FAIL at connect |
| 5. Dataset correctness | skipped in both runs — the ladder only checks a dataset it saw `prepare` finish |
| 6. the four `oltp_*` workloads, both `--db-ps-mode=disable` and default | TLS run: **all eight ran** against the table rung 4 left behind. Index run: all FAIL at connect |
| 7. sysbench's own statements driven by hand | TLS run 16/4; index run **21 accepted, 3 refused** (was 17/3), the 3 being `AUTO_INCREMENT` |

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
was still refused on the tree this run measured. `oltp_read_only`'s range and
`SUM(k)` queries therefore scan where a real sysbench run would seek, so these
numbers are not comparable to a published figure or to the Go server. Blocker 2
has since landed; no run with the index present has been taken.

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
served, and `RequireSecureTransport` is still not consulted by the connection
path — a pre-existing gap, named here rather than papered over.

## Blocker 2 (FIXED): `CREATE INDEX` runs, backfill and all

**Update, measured 2026-08-01 on `hparser-integration` at `ed62cf95f5`.**
`CREATE INDEX` and `DROP INDEX` are catalog changes this node performs. The
statement `oltp_common.lua:238` issues now succeeds against a table that
already holds its rows, which is the case that matters — sysbench creates the
secondary index AFTER `prepare` has loaded the table, so the backfill is the
load-bearing part and not an optimization.

Rung 7 was reordered to match sysbench (load first, index second) and now ends
with a real oracle rather than our own arithmetic:

| Check | Result |
| --- | --- |
| `CREATE INDEX k_1 ON sbtest1(k)` over 1,000 loaded rows | OK |
| `ADMIN CHECK TABLE sbtest1` **on the Go tidb-server** | OK — Go verifies every entry against every row |
| `USE INDEX (k_1)` vs `IGNORE INDEX (k_1)`, `COUNT(*), SUM(id)` | `1000 500500` both ways |
| `DROP INDEX k_1` | OK |
| `ADMIN CHECK TABLE` after the drop | OK — no stale entry left behind |

Rung 7 totals moved from 17 accepted / 3 refused to **21 accepted / 3
refused**; the three refusals are all blocker 3 (`AUTO_INCREMENT`).

`--create-secondary=off` is no longer needed, so `oltp_read_only`'s range and
`SUM(k)` queries keep the index they exist to exercise.

Two things this deliberately does not do, both stated in
`tidb_exec::cluster_ddl`'s module doc:

* Go's `delete only` -> `write only` -> `reorg` -> `public` ladder is not
  ported. The index and every entry the existing rows owe it become visible at
  ONE commit, so a row another writer commits between the DDL transaction's
  `start_ts` and its commit is indexed by neither the scan nor that writer.
  This widens the node's existing single-writer assumption from "no concurrent
  DDL" to "no concurrent WRITE to the table being indexed", and unlike the DDL
  half it is not enforced by a write conflict.
* Index shapes whose entries this node would not go on to maintain — prefix,
  expression, partial, `GLOBAL`, `FULLTEXT`/`SPATIAL`/`VECTOR` — are refused at
  admission, before a timestamp is spent. Publishing one would write a
  `TableInfo` this node's own catalog loader then drops, so the table would
  vanish from the connection that indexed it.

Worth recording: on this machine the **Go** control run could not create the
index at all (`error 8256: Check ingest environment failed: no enough space in
/tmp/tidb/tmp_ddl-45000`), while the Rust node could. Go's add-index goes
through the ingest/lightning path and needs that temp directory; this node's
backfill writes entries through the ordinary 2PC and needs no local disk.

## Blocker 3 (FIXED): `AUTO_INCREMENT` is served

**Update.** The node now serves `AUTO_INCREMENT`, so sysbench's default
`sbtest1` schema (`id INTEGER NOT NULL AUTO_INCREMENT ... PRIMARY KEY (id)`)
loads without `--auto-inc=off`. Both earlier states are gone: the silent
unusable table, and the honest `CREATE`-time refusal that replaced it.

What changed is WHERE the counter lives. It was a process-local `AtomicU64`
starting at zero, which is right for the in-process tier and wrong against
shared cluster storage — a second node, or the same node after a restart,
would re-issue ids that already exist. It now has the home Go gives it:

* `pkg/meta/meta.go`'s allocator meta key, read and written in a transaction
  of its OWN (`tidb_exec::cluster_auto_id::ClusterAutoIdStore`), so an id is
  burned when it is issued and never returned by a rollback — and a node
  restart resumes from the stored value instead of from zero.
* Ranges of `autoid.GetStep()` (30000) ids at a time, held by ONE registry per
  node (`tidb_server::cluster_auto_id_seam::ClusterTableAutoIds`) rather than
  per session or per catalog rebuild, which is where Go keeps its allocators
  too (on the domain, not on the `TableInfo`).

**Which key** is the part that is easy to get backwards, and getting it wrong
would be undetectable: Go sends an AUTO_INCREMENT column to the `IID:` key
only when `TableInfo.SepAutoInc()` — `AUTO_ID_CACHE 1`. Every ordinary table
allocates from `TID:`, the same key `_tidb_rowid` uses. A node that chose
`IID:` by name would keep a counter no Go `tidb-server` reads, and the two
would hand out the same ids from separate counters. The choice is made once,
in `cluster_auto_id::auto_id_key_for`, from the stored `TableInfo`.

`CREATE TABLE ... AUTO_INCREMENT = n` seeds the key with `n - 1` inside the
DDL's own transaction, which is Go's `handleAutoIncID`.

### What was measured, against a Go `tidb-server` on the SAME cluster

`tiup playground v8.5.6` (1 PD, 1 TiKV, 1 Go `tidb-server`), Rust node in
`--cluster-session`, both writing one `probe.t`:

| Check | Result |
| --- | --- |
| `INSERT` without the column | ids `1, 2, 3, 4` ascending |
| `LAST_INSERT_ID()` after a 3-row insert | `2` — the FIRST id of the statement, as Go reports |
| explicit id `100` (above), then allocate | `101` — the explicit value rebased the counter |
| explicit id `7` (below), then allocate | `102` — unchanged, as Go's `Rebase` ignores it |
| **Go server inserts on the same table** | got `30001` — its own reserved range, no collision |
| **Rust node RESTARTED, then allocate** | `60001` — above everything issued; the counter is in TiKV, not the process |
| a third node instance, then allocate | `90001` — still climbing, one `step` per reservation |
| 4 concurrent sessions x 15 inserts | 60 rows, 60 distinct ids, `1..60`, 15 per writer |
| `CREATE TABLE ... AUTO_INCREMENT=500` | first row lands on `500` |
| Go's `SELECT id, v FROM probe.t` | byte-identical to the Rust node's answer |

The 30000-sized jumps are Go's own behaviour, not a defect: each server
reserves `autoid.GetStep()` ids at a time, so ids are ascending and unique but
not dense across servers or restarts. A Go-only cluster does the same.

### Known gaps this measurement exposed

* **`SHOW CREATE TABLE` does not print the `AUTO_INCREMENT=` clause.** Go, on
  the same table, prints `... COLLATE=utf8mb4_bin AUTO_INCREMENT=30500`; the
  Rust node stops at the collation. The stored counter is correct — Go reads
  it and continues from it — so this is a display gap in `SHOW CREATE TABLE`,
  not a counter gap. Not fixed here; it belongs to the `SHOW` surface.
* **A non-numeric `AUTO_INCREMENT` column was admitted** and produced a table
  whose every `INSERT` failed to decode. The blanket refusal had hidden it.
  Fixed with Go's own rule (`preprocessor.checkAutoIncrementOp`), whose
  allowed list is wider than "integer" — `FLOAT` and `DOUBLE` are in it, and a
  Go `tidb-server` really does accept `id DOUBLE NOT NULL AUTO_INCREMENT`. The
  refusal is now byte-identical to Go's:
  `ERROR 1105 (HY000): Incorrect column specifier for column 'id'`.

### Original report: an `AUTO_INCREMENT` table is created and then not served

sysbench's default `id INTEGER NOT NULL AUTO_INCREMENT` was accepted by
`CREATE TABLE` — and the resulting table was then invisible:

```
OK   create-table-auto-inc
FAIL auto-inc-insert-without-id: ERROR 1105 (HY000): table not found in catalog
FAIL auto-inc-select:            ERROR 1105 (HY000): table not found in catalog
```

The catalog loader skipped such tables by design, reporting at startup that a
column "is AUTO_INCREMENT, whose ids come from the cluster's own autoid
allocator, which this node does not consume". That skip was the honest half of
a wart — the node had no allocator to consume — and the fix was to give it
one, not to remove the skip.

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
2. ~~`CREATE INDEX` in the DDL surface, so `prepare` runs unmodified.~~ Done —
   see blocker 2 above, verified by Go's own `ADMIN CHECK TABLE`.
3. `--auto-inc=off` remains the documented configuration: the create/serve
   mismatch is resolved as a refusal, and serving the clause needs the
   persistent allocator unit described under blocker 3.
4. **A ladder run on a tree carrying both fix 1 and fix 2.** Neither run above
   had the other's fix, so nothing here yet shows `prepare` completing — which
   is also what gates rung 5's Rust-vs-Go checksum and what makes the rung-6
   numbers comparable, since they were taken with no secondary index.

With `--create-secondary=off` a number is available today, and the table above
is it.

## Unrelated environment note

The Go control rung fails, but on the Go side and for a local reason:
`CREATE INDEX` returns `error 8256: Check ingest environment failed: no enough
space in /tmp/tidb/tmp_ddl-<port>`. That is this machine's disk, not a TiDB or
Rust defect; it reproduced on both 2026-08-01 runs. A Go-side baseline to
compare the rung-6 numbers against will need space freed for the playground's
DDL temp directory.

In the same run, Go could not create the index and the Rust node could — not a
correctness claim about either, just a difference in where the work stages:
Go's add-index reorg goes through that local temp directory, while this node's
backfill writes its entries through the ordinary 2PC and needs no local disk.
