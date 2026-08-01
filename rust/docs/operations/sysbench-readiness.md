# Can the Rust SQL node run sysbench?

**Verdict: yes, unmodified. Stock `sysbench` with no flags of ours connects,
runs `prepare` to completion — `AUTO_INCREMENT` table, 1,000 rows, secondary
index — and then runs all four `oltp_*` workloads against this node. The
`--auto-inc=off --create-secondary=off` workarounds are retired.**

The answer is now about speed, not capability. **Update 2026-08-02
(`a30ddd25dc`, offset 33000): the `MaxUint64` point-get shortcut now fires on
the served `--cluster-session` path. `oltp_point_select` fell from 1.005 TSO
per statement to 0.0034 — Go's own rate in the same run is 0.0021 — and the
per-statement excess fell from 123.73 µs to 67.17 µs.** The same run also ran
the check no mock can: a live racing lost-update test. **It fails — but it
fails identically on a binary built before the shortcut existed, so the loss is
a pre-existing autocommit-write-path bug and not this change's.** See "The
shortcut on the path sysbench actually runs (task #165)".

**Update 2026-08-01
(`8eacf363e5`): the clustered-primary-key range extraction landed and
`oltp_read_only` went from 11.70 to 89.15 tps, 7.6x, on the same table shape.**

**Update 2026-08-01, `cfd6f963a9`: the transaction seam's three unverified
changes are now verified against a real cluster, and all three hold.** Ladder
at port offset 38000, controlled A/B and TSO scrape at port offset 37000,
snapshot-semantics probe at port offset 36000 — all three on `tiup playground
v9.0.0-beta.2.pre-nightly`. Taking the correctness-sensitive ones first:

* **The pinned thread pool does not leak a snapshot between statements.**
  This is the check the cluster-free unit could not make, because
  `RealOptimisticTransactionOpener` needs a live `PdClient`. Two consecutive
  AUTOCOMMIT statements on one connection do NOT share a timestamp — a row a
  second connection committed between them appears, as it must; two statements
  inside one explicit transaction DO share one — the same racing row is
  correctly invisible. Both facts hold over the text protocol and again over
  prepared statements, six checks, zero failures. **Rung 5's Rust-vs-Go
  checksum still matches** at `1000 500500 501715 1 1000`, and the post-run
  re-check at `1000 500500 504083` agrees on both sides, so the seam change
  altered no row set. See "Snapshot semantics under the pinned thread pool".
* **The prepared `BEGIN` fix is confirmed on the instrument that exposed it.**
  Binary-prepared `oltp_read_only` took **16.11 TSO per transaction** before;
  it now takes **1.07**, against 1.08 for text. `oltp_write_only` 7.04 -> 1.04
  and `oltp_read_write` 21.15 -> 1.12, each landing on its own text rate. A
  prepared `ROLLBACK` now discards its buffer rather than publishing it.
* **The ~17.5 µs/statement did move throughput, by about the predicted
  amount — and the standing is not redrawn.** Every within-run Rust-vs-Go
  ratio improved, `oltp_point_select` text 2.24x -> 2.05x and `oltp_write_only`
  text 2.43x -> 1.95x, but no workload moved by enough to change what the
  remaining gap is made of. The honest measure is below, and it is deliberately
  not the raw tps.

Rung 6 stayed **eight of eight** with `ignored errors: 0` in every log, and
rung 7 stayed **24 accepted, 0 refused**, both Go `ADMIN CHECK TABLE` oracles
included. Prefix-index reads (`2ca4dc2dcd`) also landed since the last run and
changed no rung's behaviour.

**Update 2026-08-01, `de899c65d1`, ladder at port offset 45000: the error 2014
regression is GONE and rung 6 ran EIGHT of eight.** `oltp_read_only` binary
prepared measured 178.82 tps and `oltp_read_write` binary prepared 52.44 tps,
the two cells that aborted last run; rung 5's Rust-vs-Go checksum still matches
at `1000 500500 501715 1 1000`, so the protocol fix altered no row set. The
`FAIL, error 2014` cells below are corrected to those numbers.

**Update 2026-08-01, `dd97293671`, ladder at port offset 42000 and probe at
port offset 43000: the write path now plans `Point_Get`, the `kv_scan` RPCs are
gone — and the throughput did not move.** This is the negative result the
task #114 prediction was written to expose, and it is reported first because it
redirects the work: `kv_scan` per single-row write fell from ~1.0 to ~0.03 and
`kv_get` rose to match, exactly as predicted, while the within-run Rust-vs-Go
gap on the same three shapes stayed at 2.31x / 2.97x / 1.83x against 2.11x /
2.90x / 1.90x before. **RPC shape was not what the remaining 2x was made of.**
See "The write path: `Point_Get` landed, the gap did not close (task #115)".

The same run measured `oltp_point_select` for the first time (task #117) and
found the cost is **not** in the access path, the storage round trips, or
result-set encoding — all three are ruled out by measurement — but in a
per-statement floor that `SELECT 1` already pays. See "Where
`oltp_point_select` actually goes (task #117)".

**A second run the same day (port offset 44000) made the Rust-vs-Go comparison
controlled — both engines on ONE `v9.0.0-beta.2.pre-nightly` cluster — and
found the cause of the write gap by measurement.** The 9.8x on
`oltp_write_only` survives the controlled A/B (9.97x), so it is not a version
artifact; and `EXPLAIN` plus TiKV gRPC counters agree that **DML by primary key
does not take the point-get path**: `UPDATE`/`DELETE ... WHERE id=?` plans as
`TableFullScan`, costing ~6 `kv_scan` RPCs per single-row statement, while the
identical predicate in a `SELECT` reaches `Point_Get`. See "The write path:
per-statement breakdown" below.

Every statement `sysbench`'s `oltp_*` workloads issue is accepted and answered
correctly, with results byte-identical to a real Go `tidb-server` reading the
same TiKV. All three of the blockers that stood around that SQL are now fixed
on this branch: the node serves inbound TLS on the MySQL port, so its
capabilities read `CLIENT_SSL=yes` and MariaDB Connector/C stops refusing it
(blocker 1); `CREATE INDEX` and `DROP INDEX` are catalog changes this node
performs, backfill included, verified by Go's own `ADMIN CHECK TABLE`
(blocker 2); and `AUTO_INCREMENT` allocates from the cluster's own counter
(blocker 3).

**Superseded by the range-fix run.** The rung table and the throughput figures
below were re-measured on 2026-08-01 at **`8eacf363e5`**, the first tree
carrying the clustered-handle range fix, port offset 43000; a Go throughput
baseline was taken the same day at port offset 44000. The description that
follows is of the earlier combined run and is kept for provenance.

**This is a combined measurement.** Measured 2026-08-01 on `hparser-integration`
at **`4634a071e7`**, the first tree carrying all three blocker fixes together
(inbound TLS `1bef5f545e`, `CREATE INDEX` `275d296dba`, `AUTO_INCREMENT`
`5c80c08269`). Release build, macOS arm64, against a `tiup playground v8.5.6`
cluster (1 PD, 1 TiKV, 1 Go `tidb-server`), Rust node in `--cluster-session`,
port offset 43000.

Every rung result in the table below comes from that one run. It supersedes
the earlier per-fix runs, each of which had exactly ONE of the three fixes
present and so stopped for a reason the others had already fixed: the
2026-07-30 run (pre-TLS) could not connect at all; the TLS run stopped at
`CREATE INDEX`; the index and auto-increment runs stopped at connect. Those
runs' findings are kept in the blocker sections below, dated, because each is
still true evidence about its own fix — but they are no longer the current
end-to-end state.

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
| 3b. Capability probe | `rust: 0x00158a08 CLIENT_SSL=yes`, `go: 0x0015aeaf CLIENT_SSL=yes` — unchanged from the TLS run |
| 3c. Go control: `sysbench prepare` against the Go `tidb-server` | **FAIL again on the `v8.5.6` playground the ladder starts**: `error 8256 ... no enough space in /tmp/tidb/tmp_ddl-47000` at `CREATE INDEX`. Cleared out-of-band by a newer playground — a full Go baseline now exists, see "The Go control rung" below |
| 4. `sysbench oltp_read_only ... prepare` | **OK on the FIRST attempt, `--auto-inc=on`** — sysbench's own default schema. Table created, 1,000 rows inserted, secondary index created, all unmodified |
| 5. Dataset correctness, Rust vs Go on the same TiKV | **OK, and identical again at `a30ddd25dc`, offset 33000, with the point-get shortcut now firing on the served path:** Rust `1000 500500 501715 1 1000`; Go `1000 500500 501715 1 1000`, post-run re-check `1000 500500 502340` on both sides. **The shortcut altered no row set.** Earlier, at `cfd6f963a9`, offset 38000, with the pinned thread pool and the prepared-`BEGIN` fix in the seam:** Rust `1000 500500 501715 1 1000`; Go `1000 500500 501715 1 1000`, post-run re-check `1000 500500 504083` on both sides. **The transaction-seam changes altered no row set.** Earlier, at `dd97293671`, offset 42000, with the write path on `Point_Get`:** Rust `1000 500500 501715 1 1000`; Go `1000 500500 501715 1 1000`, and the post-run re-check `1000 500500 503191` on both sides. **The write-path change altered no row set** — this is the gate a write-path change has to clear, and it cleared it. Earlier, at `de899c65d1`: Rust `1000 500500 501715 1 1000`; Go `1000 500500 501715 1 1000` — the same figures as the range-fix run. Neither the range fix nor the binary-protocol fix altered a row set. The post-run re-check also agreed, `1000 500500 502334` on both sides |
| 6. the four `oltp_*` workloads, both `--db-ps-mode=disable` and default | **EIGHT of eight again, 2026-08-02 at `a30ddd25dc`, offset 33000**, no aborted cell, with the point-get shortcut firing. Also eight of eight at `cfd6f963a9`, offset 38000, `ignored errors: 0` in all eight logs, secondary index present, both `--db-ps-mode` settings: the `error 2014` fix is still holding after the transaction-seam changes. Also eight of eight at `dd97293671`, offset 42000, `ignored errors: 0` in all eight logs and no `FAIL` cell: the `error 2014` fix is still holding after the write-path change. Also eight of eight at `de899c65d1`, offset 45000, secondary index present, `ignored errors: 0` in every log; and six of eight at `8eacf363e5`, where the two binary-prepared cells aborted with error 2014 — see "Regression found by this run, and closed") |
| 7. sysbench's own statements driven by hand | **24 accepted, 0 refused** — unchanged at `cfd6f963a9`, offset 38000, with prefix-index reads also landed, and at `dd97293671`, offset 42000, and at `de899c65d1` before it (was 21/3, and 16/4 before that). Includes both `ADMIN CHECK TABLE`s on the Go server, and `USE INDEX` vs `IGNORE INDEX` agreeing at `1000 500500` |

Rung 5 is the row that had never been produced: the ladder gates it on a
`prepare` that returned success, and no prior run had one. `SUM(k)` differs
from the banked `506087` only because `prepare` randomises `k`; rung 7's
deterministic 1,000-row load reproduced the banked figures exactly —
`1000 500500 506087 1 1000` after load and `1000 500500 505171` after the
write/txn statements, agreeing with Go both times.

### The shortcut on the path sysbench actually runs (task #165)

**2026-08-02 on `hparser-integration` at `a30ddd25dc`, port offset 33000, ONE
`tiup playground v9.0.0-beta.2.pre-nightly` cluster.** Both engines measured
inside that single run, ten seconds each window, `--threads=1
--table-size=1000`, release build at that tip. Artifacts under
`run-33000/`. **Every line in this section is keyed to offset 33000**; nothing
here comes from any other run, and nothing here was appended to a shared log.

**The shortcut now fires on `--cluster-session`, and the TSO rate collapsed to
Go's.** The offset-39000 run found the guard unreachable in the served mode
because the snapshot was bound before the plan existed; the statement-level
shape declaration (`c4751d4e5b`) is the channel that fixed it, and this is the
first run in which the served path takes the branch.

| Measure | before (offset 43700, `707fccdf00`) | after (offset 33000, `a30ddd25dc`) |
| --- | --- | --- |
| `oltp_point_select` **TSO/stmt**, `disable` | **1.005** | **0.0034** |
| `oltp_point_select` **TSO/stmt**, `auto` | **1.004** | **0.0031** |
| Go's TSO/stmt in the same run, `disable` / `auto` | 0.002 / 0.002 | 0.0021 / 0.0018 |
| `oltp_point_select` **per-statement excess**, `disable` | **123.73 µs** | **67.17 µs** (184.03 vs 116.86) |
| `oltp_point_select` **per-statement excess**, `auto` | **130.17 µs** | **76.80 µs** (180.48 vs 103.68) |

**The Rust node's TSO rate for `oltp_point_select` is now inside Go's own
band** — 0.0034 against Go's 0.0021 in the same run, where the residue on both
sides is background traffic on the shared PD. The excess fell by **56.6 µs**
(`disable`) and **53.4 µs** (`auto`), which is the same size as the
`PdClient::get_timestamp` round trip that was being charged to every statement.
Go's per-statement total is the fixed point that makes this comparable across
runs, and it landed where it always does: **116.86 / 103.68 µs**, against the
117–130 µs band of the five prior runs.

#### The counter, and why it is sound in THIS run

PD exports no timestamps-handed-out counter — the inventory dumped from
`/metrics` at offset 33000 contains only `pd_cluster_tso_*` latency histograms
and the gRPC message counters. So the figures come from
`grpc_server_msg_received_total{grpc_method="Tso",grpc_service="pdpb.PD"}`,
which counts **Tso messages, not timestamps**: batching (`5e725476af`) coalesces
already-queued waiters into one `count`-wide request. At `--threads=1` there is
never a second waiter, so every batch should be `count = 1` — and this run
**calibrates that rather than assuming it**, with five probes whose answers are
known integers:

| Calibration probe | Engine | Tso delta | Expected |
| --- | --- | --- | --- |
| 100 explicit read-only transactions | Go | **102** | ~101, one timestamp each |
| 100 explicit 2PC write transactions | Go | **203** | ~200, `start_ts` + `commit_ts` |
| 200 autocommit PK point gets | **Rust** | **1** | 0 if the shortcut fires |
| 200 autocommit range reads (`id BETWEEN`) | **Rust** | **200** | ~200, the negative control |
| 200 autocommit PK `UPDATE`s | **Rust** | **401** | NOT 0 — a write must never take it |

A counter that under-counted coalesced timestamps could not land on 2.03 for a
workload known to take exactly two. The third and fourth rows are the
shortcut's own signature measured directly, away from sysbench: **200 point
gets cost one timestamp between them, and 200 ranges cost 200.** The fifth row
is the guard, live: an autocommit `UPDATE` by primary key still pays its two,
so no write took the branch.

#### Rung 5 and rung 6 in the same run

**Rung 5's Rust-vs-Go checksum still matches** at `1000 500500 501715 1 1000`
on both sides, and the post-run re-check agrees at `1000 500500 502340` on both
sides. **The shortcut altered no row set.** **Rung 6 ran eight of eight**, all
four workloads in both ps modes, no aborted cell.

#### All eight Rust cells and the Go control, offset 33000

| Workload | ps mode | Rust µs/stmt | Go µs/stmt | Rust excess | Rust TSO/stmt | Go TSO/stmt |
| --- | --- | --- | --- | --- | --- | --- |
| `oltp_point_select` | disable | 184.03 | 116.86 | **67.17 µs** | **0.0034** | 0.0021 |
| `oltp_point_select` | auto | 180.48 | 103.68 | **76.80 µs** | **0.0031** | 0.0018 |
| `oltp_read_only` | disable | 289.89 | 125.98 | 163.91 µs | 0.0679 | 0.0648 |
| `oltp_read_only` | auto | 273.69 | 169.68 | 104.01 µs | 0.0671 | 0.0661 |
| `oltp_write_only` | disable | 352.66 | 184.35 | 168.31 µs | 0.1728 | 0.3365 |
| `oltp_write_only` | auto | 362.38 | 528.79 | (Go window noisy) | 0.1728 | 0.3423 |
| `oltp_read_write` | disable | 403.59 | 677.72 | (Go window noisy) | 0.0567 | 0.1112 |
| `oltp_read_write` | auto | 385.56 | 230.54 | 155.02 µs | 0.0569 | 0.1041 |

Two Go windows in this run (`oltp_write_only auto`, `oltp_read_write disable`)
came in far outside their own band and are **not** reported as excesses; the Go
point-select windows, which are the ones this section turns on, landed squarely
inside it. Only `oltp_point_select` is autocommit per statement, so only it can
take the shortcut — the other three run inside explicit transactions and their
TSO rates are unchanged, exactly as they should be.

#### The check a mock cannot do: a real lost update — and what it caught

The mock cluster's max-ts snapshot is a clone of the committed store, so it can
say by counter which branch a statement took but can never show a wrong row.
Run live against a racing writer, the shape of
`an_updates_read_before_write_does_not_take_the_shortcut` **loses updates.**

**But it is not this change's loss.** The same experiment on a binary built at
`60f06c5224` — the commit immediately BEFORE the shape declaration, with no
`declare_autocommit_point_get` in the tree at all — loses the same amount. The
attribution matrix, 300 blind `v = v + 5` / `v = v + 7` statements per side on
one row, arithmetic checked as `100 + 5·accepted + 7·accepted`:

| Variant | after (`a30ddd25dc`, offset 34000) | before (`60f06c5224`, offset 35000) |
| --- | --- | --- |
| Go vs Go — the method's own control | **PASS** 3700 = 3700 | **PASS** 3700 = 3700 |
| Rust vs Go, `WHERE id = 1` | **LOST** 2538 vs 3700, short 1162 | **LOST** 2582 vs 3695, short 1113 |
| Rust vs Rust | **LOST** 1926 vs 3700, short 1774 | **LOST** 2142 vs 3700, short 1558 |
| Rust vs Go, `WHERE id = 1 AND v > -1000000` | **LOST** 2498 vs 3695, short 1197 | **LOST** 2461 vs 3700, short 1239 |
| Rust alone, uncontended | **PASS** 1600 = 1600 | **PASS** 1600 = 1600 |

Four things are settled by that table. The **Go-vs-Go row is exact**, so the
arithmetic is a valid oracle and not the thing that is wrong. The
**uncontended row is exact**, so nothing is dropping writes outside a race. The
**non-point-shape row loses the same amount**, and that predicate is one
`statement_read_shape` refuses outright — so **the point-get shape is not the
variable**. And the **before column loses the same amount as the after column**,
which is the direct answer: **the shortcut did not cause this, and the guard
that keeps writes off it holds.** The counter agrees with the row: 200
autocommit PK `UPDATE`s cost 401 timestamps at offset 33000, so no `UPDATE`
took the branch.

**What does cause it is one line above the autocommit publication, and the code
already says so.** `commit_staged_buffer`
(`crates/tidb-exec/src/cluster_table_storage.rs:665-692`) documents itself:

> the statement's own read transaction has already ended, so the publication
> takes a fresh timestamp

So an autocommit `UPDATE` reads at `T_read` and prewrites under a *different,
later* `T_write`. TiKV's optimistic conflict check at prewrite is against the
prewriting transaction's `start_ts`, so a commit landing in the window
`T_read < commit_ts < T_write` is **not a conflict TiKV can see**, and the value
computed from the stale read overwrites it with no error. Go's implicit
per-statement transaction takes **one** timestamp and reads and prewrites at it,
which is exactly why the Go-vs-Go row is exact. The comment's claim that this is
"exactly as Go's implicit per-statement transaction does" is the part that is
not true: Go does not re-timestamp between the read and the write.

#### The Go source, quoted — the premise holds

The claim above was checked against the Go before any Rust was touched, because
a premise that is wrong here would change the whole shape of the fix. It is
right. For an optimistic (autocommit) statement the **read** timestamp is
literally the transaction's `start_ts`:

```go
// pkg/sessiontxn/isolation/optimistic.go:45-46
p.getStmtReadTSFunc = p.getTxnStartTS
p.getStmtForUpdateTSFunc = p.getTxnStartTS
```

```go
// pkg/sessiontxn/isolation/base.go:268-274
func (p *baseTxnContextProvider) getTxnStartTS() (uint64, error) {
	txn, err := p.ActivateTxn()
	...
	return txn.StartTS(), nil
}
```

Note that `GetStmtForUpdateTS` — the timestamp an `UPDATE`/`DELETE`/`INSERT`
reads its rows at — is the *same function* under optimistic isolation. There is
no second allocation between the read and the write. `ActivateTxn`
(`base.go:283`) returns the already-activated `p.txn` if there is one, so the
whole statement sees a single activation.

And the **prewrite** goes out under that same number, unchanged:

```go
// client-go v2.0.8-0.20260708122311-01bd8f99f4da txnkv/transaction/2pc.go:471-474
committer := &twoPhaseCommitter{
	store:   txn.store,
	txn:     txn,
	startTS: txn.StartTS(),
```

```go
// .../txnkv/transaction/prewrite.go:174
StartVersion: c.startTS,
```

So Go allocates exactly **one** timestamp for an autocommit DML statement's read
*and* its prewrite; the only other timestamp it spends is the `commit_ts` after
prewrite succeeds. That is why TiKV's `commit_ts > start_ts` conflict check is
sufficient for Go and insufficient for us: our prewrite carries a `start_ts` the
read never saw. **Premise confirmed, not falsified.**

This was a **pre-existing data-loss bug in the autocommit write path**,
reproducible on both sides of task #165 and independent of it. It is **fixed**;
the measurement that fixed it is below.

#### Fixed, and measured on a real cluster — offset 43000

**2026-08-02 on `hparser-integration`, port offset 43000, ONE
`tiup playground v9.0.0-beta.2.pre-nightly` per run, `--cluster-session`
release build.** Harness: `rust/scripts/run-lost-update-check.sh`. Two sessions,
one row, `UPDATE lu SET v = v + 5` and `+ 7` run 300 times each, blind, so the
arithmetic is the oracle. Statements the server *refused* are counted and
subtracted, because a refusal wrote nothing — that is what makes the total exact
whether or not the fix reports conflicts.

Two runs on the same harness at the same offset, differing only in one line of
`commit_staged_buffer` — whether the publication takes a fresh PD timestamp or
the statement's own read timestamp:

| Scenario | Before (fresh write ts) | After (publish at the read ts) |
| --- | --- | --- |
| **control** Go vs Go (method control) | **PASS 3600 = 3600**, 0 refused | **PASS 3600 = 3600**, 0 refused |
| **control** Rust alone, uncontended (no-race control) | **PASS 1500 = 1500** | **PASS 1500 = 1500** |
| Rust vs Go, point predicate | **LOST 2487 of 3600**, 0 refused | **PASS 2850 = 2850**, 150 refused |
| Rust vs Rust, point predicate | **LOST 1669 of 3595**, 1 refused | **PASS 2154 = 2154**, 117 + 123 refused |
| Rust vs Go, ranged predicate | **LOST 2410 of 3600**, 0 refused | **PASS 2875 = 2875**, 145 refused |

The before column reproduces the original figures (2538 / 1926 / 2498) within
run-to-run spread, so the harness does see the bug; the after column reaches the
exact total in every scenario, with **both controls still exact**. The ranged
predicate — one `statement_read_shape` refuses outright — moves with the rest,
so the fix is not scoped to the point-get shape.

**What changed in kind, and it is not free.** Before, a statement that lost the
race reported success and silently discarded another session's commit. After, it
reports `[kv:9007] Write conflict` and writes nothing. Note the Go control
refuses **zero** of its 300 statements under the same contention: Go's autocommit
statement retries the conflict internally, so its client never sees it
(`@@tidb_retry_limit` was `10` on the measured cluster). So the *loss* is closed
and the remaining divergence is an error surface: **we refuse where Go retries**.
That is a correctness improvement with a visible cost, and the retry is the next
piece of work, not part of this one.

#### The rarer refusal, task #168: a real pessimistic lock, not a misdecode

`ERROR 1105 ... invalid Prewrite lock observation: pessimistic lock type 5 is
outside bounded recovery` appeared 3 times across 1,500 statements in the fixed
run. **The decoder is not wrong.** `crates/tidb-txnkv/src/lock/model.rs:19-26`
maps `Put=0, Del=1, Lock=2, Insert=4, PessimisticLock=5, CheckNotExists=6`,
which is byte-for-byte kvproto's `Op` enum
(`kvproto@v0.0.0-20251104104744/pkg/kvrpcpb/kvrpcpb.pb.go:290-319`). No
off-by-one, no wrong base: a `5` is a `PessimisticLock` and we read it as one.

So a real pessimistic lock is being met, and the run says whose. Every one of
the three occurrences is on the **Rust** side of a **Rust vs Go** race; there are
zero in Rust-vs-Rust, zero in Go-vs-Go, and zero uncontended. Nothing on the
`--cluster-session` path takes a pessimistic lock — `begin_pessimistic` has one
caller, `MultiStatementTransaction`, which that node never constructs — so the
lock is the Go server's, on the one row both sides write. (The measured cluster
reports `@@tidb_txn_mode = pessimistic` with
`pessimistic-txn.pessimistic-auto-commit = false`, so *which* Go path takes it is
still open.)

What is settled: this is not a decoding bug, and the gap is that an optimistic
prewrite of ours cannot resolve a pessimistic lock it meets, where client-go
would. The message is left in place.

### The `MaxUint64` point-get shortcut on a real cluster (task #142)

**2026-08-01 on `hparser-integration` at `63acc31852`, port offset 39000, ONE
`tiup playground v9.0.0-beta.2.pre-nightly` cluster.** Both engines measured
inside that single run, ten seconds each window, `--threads=1
--table-size=1000`, release build rebuilt at that tip. Artifacts:
`maxts-pointget-39775-1785592241`. Every line in this section is keyed to
offset 39000; nothing here comes from any other run.

**Headline, stated first because it is the disappointing one: the excess did
NOT fall, and the TSO rate did not fall either.** `oltp_point_select` still
costs the Rust node **1.005 TSO per statement** (`--db-ps-mode=disable`) and
**1.004** (`auto`), against Go's 0.002 in the same run — identical to the
pre-change figures. The per-statement excess is **126.12 µs** (259.83 vs
133.71) and **127.62 µs** (245.30 vs 117.68), against **123.73 / 130.17 µs**
before the change. Within this machine's run-to-run spread, nothing moved.

**Root cause: the shortcut is not on the path sysbench runs.**
`MAX_TS_POINT_GET_SNAPSHOT` is consulted in
`RealTiKvReader::execute_lowered_plan_with_cancellation`
(`crates/tidb-exec/src/real_tikv_read.rs:1265-1273`), reached from
`real_tikv_node`'s `execute_read`. The node the ladder and every sysbench run
start is `--cluster-session`, a different session driver:
`ClusterServerSession::run_statement`
(`crates/tidb-server/src/cluster_session_node/mod.rs:495-500`) binds a snapshot
**before the statement is planned** —

```rust
let snapshot = match self.explicit.as_ref() {
    Some(transaction) => transaction.snapshot(),
    None => self.transactions.open_snapshot(),
};
```

— and `open_snapshot` (`cluster_session_node/transactions.rs:104`) opens a
`StatementSnapshot`, which takes a timestamp unconditionally. At that point the
plan shape the guard tests does not exist yet, so no autocommit point get in
`--cluster-session` can ever reach the shortcut. **The change is real and
tested in its own crate; it simply does not reach the served mode.** Making it
pay requires deferring the autocommit snapshot bind until after planning, which
is a session-driver change, not an executor one.

#### Does real TiKV honour `version = u64::MAX`? Yes — offset 40000

**Second run, 2026-08-01 at `63acc31852`, port offset 40000, its own
`tiup playground v9.0.0-beta.2.pre-nightly`.** Artifacts:
`maxts-bound-89696-1785592702`. The offset-39000 run could not answer this: its
node runs `--cluster-session`, which never sends `MaxUint64` at all, so its
checks exercised the per-statement-timestamp path and prove nothing about the
shortcut. This run starts a **second Rust node bound with `--load-table
sbtest.sbtest1`**, which is the mode `execute_lowered_plan_with_cancellation`
serves, and puts every question to it.

The dataset had to be built by hand rather than by `sysbench prepare`: the
bound node refuses `sysbench`'s own `id INTEGER` handle — *"column `id` is the
row handle but has type INT, not signed BIGINT"* — so the table carries
sysbench's exact shape with a `BIGINT` handle and is loaded by the Go server on
the same TiKV.

**Rows: yes, and they match Go's exactly.** Nine PK point gets served at
`MaxUint64` — ids 1, 2, 7, 8, 250, 500, 999, 1000, and the absent 5000 —
returned byte-identical `(id, k, c, pad)` tuples to the Go `tidb-server` on the
same TiKV, and the absent id returned the empty set on both. TiKV neither
rejected nor truncated the version, and there was no silent empty result.

**The two visibility questions, which were pinned locally only against a model
of TiKV's MVCC reader, both hold against the real one:**

| Question | Result |
| --- | --- |
| A row another session commits **between** two autocommit point reads must become visible | **PASS.** `id=7` read `c-7`, Go committed `MAXTS-VISIBILITY-MARK-1`, the next autocommit read returned the mark |
| The same pair **inside one transaction** must NOT see it | **PASS.** `id=8` read `c-8` inside `BEGIN`, Go committed `MAXTS-VISIBILITY-MARK-2`, the second read inside the same transaction still returned `c-8` |
| …and after that transaction ends, a fresh autocommit read does see it | **PASS.** Returned `MAXTS-VISIBILITY-MARK-2`, agreeing with Go |

**Nothing disagreed with the local model.** `MaxUint64` means "latest committed
version" on real TiKV exactly as the in-repo model of the MVCC reader said, and
routing an open transaction away from the shortcut is what keeps the second row
of that table true.

**The TSO signature of the shortcut, measured three ways on one node:**

| Probe | Statements | Tso delta | Reading |
| --- | --- | --- | --- |
| autocommit PK point gets | 200 (200 rows) | **0** | the shortcut fires and costs no timestamp at all |
| autocommit ranges (`id BETWEEN`) | 200 (800 rows) | **215** | the control: a non-point autocommit read still pays one each |
| PK point gets inside explicit transactions | 100 (100 rows) | **101** | one per transaction, as isolation requires |

The middle and bottom rows are also a second, independent calibration of the
PD counter after TSO batching: 100 transactions read 101, and 200 single-read
statements read 215.

**What this does and does not buy.** On the bound node the point-get timestamp
is gone — not reduced, zero. But `sysbench` cannot be pointed at that node at
all (`error 1049: Unknown database 'sbtest'`; a `--load-table` node serves one
table and no database namespace), so **no sysbench throughput figure exists for
the path where the shortcut fires**, and the path sysbench does run is
unchanged. The Go control in the same run measured 135.07 µs (`disable`) and
120.01 µs (`auto`) per statement at 0.002 TSO, consistent with every prior run.

#### TSO per statement, both engines, offset 39000

| Workload | ps mode | Rust µs/stmt | Go µs/stmt | Rust excess | Rust TSO/stmt | Go TSO/stmt | Rust TSO/txn | Go TSO/txn |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_point_select` | disable | 259.83 | 133.71 | **126.12 µs** | **1.005** | 0.002 | 1.005 | 0.002 |
| `oltp_point_select` | auto | 245.30 | 117.68 | **127.62 µs** | **1.004** | 0.002 | 1.004 | 0.002 |
| `oltp_read_only` | disable | 310.19 | 189.05 | 121.14 µs | 0.068 | 0.066 | 1.084 | 1.058 |
| `oltp_read_only` | auto | 315.66 | 201.14 | 114.52 µs | 0.068 | 0.066 | 1.088 | 1.058 |
| `oltp_write_only` | disable | 321.78 | 214.53 | 107.25 µs | 0.172 | 0.337 | 1.033 | 2.021 |
| `oltp_write_only` | auto | 1262.83 | 542.59 | 720.24 µs | 0.187 | 0.343 | 1.120 | 2.058 |
| `oltp_read_write` | disable | 364.46 | 262.94 | 101.52 µs | 0.058 | 0.105 | 1.154 | 2.094 |
| `oltp_read_write` | auto | 360.01 | 299.38 | 60.63 µs | 0.056 | 0.106 | 1.125 | 2.110 |

**Which counter, and why it is sound after TSO batching.** The figures come
from PD's own
`grpc_server_msg_received_total{grpc_method="Tso",grpc_service="pdpb.PD"}`,
scraped before and after each ten-second window. Batching (`5e725476af`)
coalesces waiters **already queued on the worker channel** into one `count`-wide
request, so that counter is no longer one-for-one with timestamps *in general*
— but at `--threads=1` there is never a second waiter to coalesce, so every
batch is `count = 1`. **That is not assumed here; the run calibrates it.** The
`TSO/txn` columns are the calibration: an explicit read-only transaction takes
exactly one timestamp and reads **1.03–1.09**; Go's 2PC write transaction takes
exactly two and reads **2.021 / 2.058 / 2.094 / 2.110**. A counter that
under-counted coalesced timestamps could not land on 2.00 for a workload known
to take two. The residue above the integer is background traffic on the shared
PD (GC, the idle engine's own ticks), which is why the point-select rows read
1.005 rather than 1.000.

**A calibration probe in this run's script was void and is not reported.** Two
extra probes (100 scripted transactions, 200 scripted autocommit point gets)
generated their SQL through a shell-quoted Python one-liner in which `%%`
reached Python literally; the statements never ran and both printed a delta of
2, which is background noise and not a measurement. The `TSO/txn` calibration
above is independent of them and stands.

### TSO per statement, all four workloads, both ps modes (task #139)

**2026-08-01 on `hparser-integration` at `707fccdf00`, port offset 43700, ONE
`tiup playground v9.0.0-beta.2.pre-nightly` cluster.** Both engines measured
inside that single run, ten seconds each, `--threads=1 --table-size=1000`.
Artifacts: `tso-collation-20344-1785589896`. Every line below is keyed to
offset 43700; nothing here comes from any other run.

**Method, and why no Rust source changed.** A TSO counter *inside* the node was
not needed: PD already exports one. `PdClient::get_timestamp`
(`crates/tidb-pd-client/src/client/mod.rs:348`) sends exactly one message on
PD's `Tso` bidi stream per call — no batching, no cache, one worker round trip
— so PD's own
`grpc_server_msg_received_total{grpc_method="Tso",grpc_service="pdpb.PD"}`
counts them one for one. Scraping `/metrics` before and after each ten-second
window and dividing by sysbench's reported query count gives TSO per statement
directly, with the Go server on the same PD as its own control.

| Workload | ps mode | Rust µs/stmt | Go µs/stmt | Rust excess | **Rust TSO/stmt** | Go TSO/stmt |
| --- | --- | --- | --- | --- | --- | --- |
| `oltp_point_select` | disable | 254.25 | 130.52 | **123.73 µs** | **1.005** | 0.002 |
| `oltp_point_select` | auto | 252.33 | 122.16 | **130.17 µs** | **1.004** | 0.002 |
| `oltp_read_only` | disable | 420.38 | 180.41 | 239.97 µs | 0.070 | 0.066 |
| `oltp_read_only` | auto | 307.84 | 142.81 | 165.03 µs | 0.068 | 0.065 |
| `oltp_write_only` | disable | 328.10 | 176.75 | 151.35 µs | 0.172 | 0.336 |
| `oltp_write_only` | auto | 315.07 | 181.32 | 133.75 µs | 0.172 | 0.337 |
| `oltp_read_write` | disable | 431.66 | 248.93 | 182.73 µs | 0.059 | 0.104 |
| `oltp_read_write` | auto | 594.35 | 238.08 | 356.27 µs | 0.060 | 0.104 |

**Yes: `oltp_point_select` really does take one TSO per statement, in both ps
modes.** The read path was written that way and does not hide it —
`execute_lowered_plan_with_cancellation`
(`crates/tidb-exec/src/real_tikv_read.rs:1241-1245`) calls
`timestamp_source.current_ts()` unconditionally for every non-contradiction
plan, and in `--cluster-session` that source is `PdTimestampSource`, a direct
`PdClient::get_timestamp`. Go's 0.002 is the contrast already confirmed in
source: `optimizeWithMaxTS` (`pkg/sessiontxn/isolation/optimistic.go:109-148`)
returns `MaxUint64` without activating the transaction when
`IsPointGetWithPKOrUniqueKeyByAutoCommit` holds, which is exactly this
workload's shape. **Go issues essentially zero TSO RPCs for `oltp_point_select`
and the Rust node issues one per statement.**

**This settles which of the two circulating numbers describes the statement.**
The Rust node's whole-statement cost on `oltp_point_select` is **254.25 µs
against Go's 130.52 µs in the same run — an excess of ~124 µs, not ~6 µs.** Note
the arithmetic that explains the confusion: the trajectory pair "Rust 124.29 vs
Go 118.27" puts a number that matches this run's *excess* (123.73 µs) beside a
number that is Go's *total*. Go's per-statement total is the fixed point on this
machine (117.40 / 118.18 / 118.27 / 130.52 / 122.16 µs across runs), and no
Rust total has ever landed near it. **The ~6 µs reading is not a whole-statement
figure and should not be quoted as one.** The cluster-free
`PdClient::get_timestamp` measurement (38.5 µs against a zero-latency mock PD)
is the number that is consistent with what was measured here: one such call is
charged to every `oltp_point_select` statement and a real PD costs more than
the mock.

**The other three workloads do not pay it per statement**, and that is not an
elision — it is the explicit transaction: each `BEGIN` pins one snapshot and
every statement inside it reads at that snapshot via
`execute_plan_at_snapshot`. `oltp_read_only` is 14 statements per transaction
and reads 0.070 (Go 0.066); `oltp_read_write` is 20 and reads 0.059 (Go 0.104).
The binary-prepared regression recorded in the task #117 section below
(16.11 TSO/txn) remains fixed: binary and text agree to three decimals on
every workload here. On the two write workloads Rust now takes **half** the
timestamps Go does (0.172 vs 0.336), because Go takes a second one to commit.

**What this does to the performance picture.** The per-statement excess on the
lightest workload is ~124 µs and one real PD round trip is inside it. That is a
single identified, removable item — Go's own `MaxUint64` shortcut for
autocommit point gets is the fix, not a micro-optimisation — and it is
plan-gated in Go exactly where this node would gate it. The task #117 finding
that ~61% of the point-select excess is a floor charged before the statement
touches a table still stands and is the larger term; the TSO round trip lives
in the remaining marginal term.

### String comparison pushdown against a REAL TiKV collator (task #137)

**Same run, same cluster, offset 43700.** Table `colltest.coll` (id 125) holds
the unit's own fixtures as two columns over the same bytes — `s` under
`utf8mb4_general_ci`, `b` under `utf8mb4_bin` — rows `(1,'A') (2,'a')
(3,'aéb') (4,'B')`, with the Go server confirming `hex(s) = 61C3A962` for the
accented row. Rows are compared against the Go `tidb-server` on the same TiKV;
the wire counts come from `cluster-session-smoke --cop` as a second process,
because a served node reports no coprocessor counters.

**Every case the in-repo fake predicted is what real TiKV produced. The rust
and Go row sets are identical in all six.**

| Statement | ids returned (Rust = Go) | rows across the wire | fake predicted |
| --- | --- | --- | --- |
| `s = 'a'` (`general_ci`) | **1, 2** | **2** of 4 | 2 |
| `b = 'a'` (`bin`) | **2** | **1** of 4 | 1 |
| `s = 'A'` (`general_ci`) | 1, 2 | 2 of 4 | — |
| `s < 'azb'` (`general_ci`) | **1, 2, 3** | 3 of 4 | 3, ids 1/2/3 |
| `b < 'azb'` (`bin`) | **1, 2, 4** | 3 of 4 | 3, ids 1/2/4 |
| `s = 'aeb'` (`general_ci`) | **3** | 1 of 4 | é weighs as e |

Every request carried `TableScan(table 125, 2 columns) | Selection(1
conditions)` and `served 1, refused 0` — the comparison travelled in all six
cases, none fell back to a local filter.

The three assertions this was built to make:

* **`'A' = 'a'` is TRUE under `general_ci` and FALSE under `utf8mb4_bin`**, at
  the region: 2 rows on the wire versus 1, from the same four bytes.
* **`s < 'azb'` returns three rows under both collations and NOT the same
  three** — `1,2,3` versus `1,2,4`. The count is identical, so only the ids
  separate them, and they separate correctly.
* **`é` weighs as `e`**: `s = 'aeb'` selects the accented row under
  `general_ci`, and it is the region that decided so (1 row crossed the wire,
  not 4).

**Nothing disagreed with the fake.** The risk this run existed to close —
our collator table diverging from TiKV's own, which a fake sharing our table
could never catch — did not materialise on these fixtures: the rows TiKV chose
to send are the rows Go's engine independently selects.

### Throughput, `--threads=1 --time=10`, **secondary index present**

**Current ladder measurement: 2026-08-01 on `hparser-integration` at
`dd97293671`**, the tree carrying the write-path `Point_Get` plan (task #115).
Release build, macOS arm64, `tiup playground v9.0.0-beta.2.pre-nightly` (server
`8.0.11-TiDB-v9.0.0-beta.2.pre-2052-g23bff31318`), Rust node in
`--cluster-session`, **port offset 42000** — every line in this table is keyed
to that offset and comes from that single run, whose artifacts are the
`u115/ladder-42000` set. All eight cells ran, `ignored errors: 0` in every log,
rung 3c passed, and rung 3b read `rust: 0x00158a08 CLIENT_SSL=yes`,
`go: 0x0015aeaf CLIENT_SSL=yes`.

| Workload | Rust text (`--db-ps-mode=disable`) | Rust binary prepared (default) |
| --- | --- | --- |
| `oltp_point_select` | 3,876.16 qps | 3,928.05 qps |
| `oltp_read_only` | 3,332.22 qps (208.26 tps) | 2,791.88 qps (174.49 tps) |
| `oltp_write_only` | 2,684.38 qps (447.40 tps) | 2,192.15 qps (365.36 tps) |
| `oltp_read_write` | 2,786.41 qps (139.32 tps) | 2,335.07 qps (116.75 tps) |

These are within noise of the `28eebaffa1` table below on every cell, which is
consistent with the controlled A/B: the write-path `Point_Get` changed the RPC
shape and not the throughput. Read the within-run ratios in "The controlled A/B
at `dd97293671`" rather than these absolutes.

#### Superseded: the same table at `28eebaffa1`, before the write-path `Point_Get`

Conditions: 2026-08-01 on `hparser-integration` at `28eebaffa1`,
the tree carrying the clustered-handle range fix, the binary-prepared
rangeless-scan fix, AND the write-path handle-range lowering (task #114).
Release build, macOS arm64, `tiup playground v9.0.0-beta.2.pre-nightly` (server
`8.0.11-TiDB-v9.0.0-beta.2.pre-2052-g23bff31318`), Rust node in
`--cluster-session`, **port offset 45000** — every `28eebaffa1` line below is
keyed to that offset and comes from that single run, whose artifacts are the
`ladder-45000-u114` set. All eight cells ran; `ignored errors: 0` and zero
`FATAL`/`ERROR` lines throughout. The secondary index `k_1` was present: stock
`prepare` created it at rung 4 and it was never dropped before rung 7.

This is also the first ladder run on a single playground version end to end.
The script pinned `v8.5.6` until this commit, which made its own rung-3c Go
control fail at `CREATE INDEX`; `SYSBENCH_CLUSTER_VERSION` now defaults to the
newer playground and **rung 3c passes for the first time**, so the control and
the measurement share one cluster version.

| Workload | Rust text (`--db-ps-mode=disable`) | Rust binary prepared (default) |
| --- | --- | --- |
| `oltp_point_select` | 3,925.30 qps | 4,075.45 qps |
| `oltp_read_only` | 3,497.91 qps (218.62 tps) | 2,823.74 qps (176.48 tps) |
| `oltp_write_only` | **2,697.10 qps (449.52 tps)** | **2,222.80 qps (370.47 tps)** |
| `oltp_read_write` | 2,750.23 qps (137.51 tps) | 2,420.79 qps (121.04 tps) |

The two bold cells are the write workload, and they are where the write-path
fix shows up. Read them only as "the write number moved a lot"; the trustworthy
figure is the within-run Rust-vs-Go ratio in "The controlled A/B (task #114)"
below, because this ladder is a different playground instance from the run that
produced the previous table and **cross-run absolutes on this machine are not
comparable** — the same read code has measured 89.15 and 219.91 tps on
consecutive runs, and Go's own `oltp_read_only` has moved 419.71 -> 249.45
between runs.

`error 2014` did not come back. Both binary-prepared cells that carried it
(`oltp_read_only`, `oltp_read_write`) ran clean again, and rung 6 was eight of
eight.

### Superseded: the same table at `de899c65d1`, before the write-path fix

Conditions: 2026-08-01, `tiup playground v8.5.6`, port offset 45000, artifacts
the `ladder-45000` set. Same shapes, same thread count, index present.

| Workload | Rust text (`--db-ps-mode=disable`) | Rust binary prepared (default) |
| --- | --- | --- |
| `oltp_point_select` | 3,933.91 qps | 4,101.05 qps |
| `oltp_read_only` | 3,518.51 qps (219.91 tps) | 2,861.11 qps (178.82 tps) |
| `oltp_write_only` | 640.08 qps (106.68 tps) | 562.00 qps (93.67 tps) |
| `oltp_read_write` | 1,194.03 qps (59.70 tps) | 1,048.70 qps (52.44 tps) |

That run is the one that closed `error 2014`: the two `oltp_read_only` /
`oltp_read_write` binary cells had read `FAIL, error 2014` before it and
carried numbers after it.

### Earlier throughput runs, superseded, conditions labelled

**2026-08-01 at `8eacf363e5`** — the first tree carrying the clustered-handle
range fix, but NOT the binary-prepared fix. Release build, macOS arm64, `tiup
playground v8.5.6`, Rust node in `--cluster-session`, **port offset 43000** —
every Rust line below is keyed to that offset and comes from that single run.
Superseded by the `de899c65d1` table above; kept because it is the run that
found the 2014 regression.

**The secondary index `k_1` existed during every measurement in this table.**
Stock `sysbench prepare` created it at rung 4 (`prepare-auto-inc-on.log`:
`Creating a secondary index on 'sbtest1'...`, no error), and an independent
read-only observer polling `information_schema.STATISTICS` **on the Go server**
reported `PRIMARY,k_1` continuously across rung 6. This matters because the
rows marked `(no index)` below are measurements of a different table shape and
are not comparable.

| Workload | Rust text (`--db-ps-mode=disable`) | Rust binary prepared (default) |
| --- | --- | --- |
| `oltp_point_select` | 3,847.51 qps | 3,372.71 qps |
| `oltp_point_select` (index, pre-range-fix, superseded) | 3,272.84 qps | 3,827.20 qps |
| `oltp_point_select` (no index, superseded) | 3,931.92 qps | 3,875.83 qps |
| `oltp_read_only` | 1,426.46 qps (89.15 tps) | **FAIL, error 2014** — fixed since; re-measured at 178.82 tps, see the `de899c65d1` table |
| `oltp_read_only` (index, pre-range-fix, superseded) | 187.25 qps (11.70 tps) | 181.67 qps (11.35 tps) |
| `oltp_read_only` (no index, superseded) | 1,292.04 qps (80.75 tps) | 1,035.92 qps (64.74 tps) |
| `oltp_write_only` | 634.18 qps (105.70 tps) | 561.90 qps (93.65 tps) |
| `oltp_write_only` (index, pre-range-fix, superseded) | 567.78 qps (94.63 tps) | 496.60 qps (82.77 tps) |
| `oltp_write_only` (no index, superseded) | 292.16 qps (48.69 tps) | 575.10 qps (95.85 tps) |
| `oltp_read_write` | 1,185.45 qps (59.27 tps) | **FAIL, error 2014** — fixed since; re-measured at 52.44 tps, see the `de899c65d1` table |
| `oltp_read_write` (index, pre-range-fix, superseded) | 172.43 qps (8.62 tps) | 147.88 qps (7.39 tps) |
| `oltp_read_write` (no index, superseded) | 927.41 qps (46.37 tps) | 847.49 qps (42.37 tps) |

**The range fix worked, and it is not a small effect.** On the same table shape
— index present both times — `oltp_read_only` went from **11.70 to 89.15
transactions per second, a 7.6x improvement**, and `oltp_read_write` from 8.62
to 59.27 tps, a 6.9x improvement. Both now also clear the old `(no index)`
figures (80.75 and 46.37 tps), so the index sysbench creates has stopped being
a net cost and started being a win, which was the whole point. Point-select is
unchanged, as expected — it never went through a range. `oltp_write_only`
improved modestly (94.63 -> 105.70 tps), consistent with its reads being point
lookups.

### The Go baseline, same shapes, same machine

**Obtained for the first time on 2026-08-01**, port offset 44000, via a
`tiup playground v9.0.0-beta.2.pre-nightly` (server version
`8.0.11-TiDB-v9.0.0-beta.2.pre-2052-g23bff31318`). Identical sysbench
invocation: `--tables=1 --table-size=1000 --threads=1 --time=10`, stock
`prepare` with the default `AUTO_INCREMENT` schema, `PRIMARY,k_1` present.

| Workload | Go text (`--db-ps-mode=disable`) | Go binary prepared (default) |
| --- | --- | --- |
| `oltp_point_select` | 7,749.02 qps | 9,408.95 qps |
| `oltp_read_only` | 6,715.43 qps (419.71 tps) | 7,935.14 qps (495.95 tps) |
| `oltp_write_only` | 6,237.96 qps (1,039.66 tps) | 6,453.74 qps (1,075.62 tps) |
| `oltp_read_write` | 4,831.79 qps (241.59 tps) | 5,341.51 qps (267.08 tps) |

Text-mode Rust-vs-Go ratios, the only column both engines completed:

| Workload | Rust | Go | Go is faster by |
| --- | --- | --- | --- |
| `oltp_point_select` | 3,847.51 qps | 7,749.02 qps | 2.0x |
| `oltp_read_only` | 89.15 tps | 419.71 tps | 4.7x |
| `oltp_write_only` | 105.70 tps | 1,039.66 tps | 9.8x |
| `oltp_read_write` | 59.27 tps | 241.59 tps | 4.1x |

**Caveat on comparability, stated rather than buried:** the Rust ladder ran
against a `v8.5.6` playground and the Go baseline against a
`v9.0.0-beta.2.pre-nightly` one, because that version difference is exactly
what made the Go run possible at all (see "The Go control rung"). The TiKV
underneath therefore differs, and the two runs were sequential rather than
simultaneous. These are the right order of magnitude, not a controlled A/B.
**That caveat is now retired: the controlled A/B below puts both engines on one
cluster.**

Still one thread, a 1,000-row table, and a laptop also running the cluster it
queries, so read the absolute values with that in mind.

### Snapshot semantics under the pinned thread pool (task #124)

**2026-08-01, `cfd6f963a9`, port offset 36000** (the text legs also ran at
offset 37000 inside the A/B run, with the same answers), one `tiup playground
v9.0.0-beta.2.pre-nightly` cluster.

The pinned thread pool replaced one OS thread per autocommit statement with a
parked worker borrowed from a pool. The failure that change could introduce is
a **parked worker carrying a transaction, and therefore a timestamp, from one
statement into the next** — which is a silent wrong answer, not a slow one, and
which no single-connection test can see. It only shows as a row another
connection committed in between going missing.

So the probe uses two connections: one reader whose consecutive statements are
the ones that would share a worker, and one writer committing a row between
them.

| Check | Required | Observed |
| --- | --- | --- |
| Two AUTOCOMMIT statements, text — racing row must APPEAR | 11 | 11 OK |
| Two statements in one explicit transaction, text — racing row must NOT appear | 11 | 11 OK |
| First statement after `COMMIT` — racing row must appear | 12 | 12 OK |
| Two AUTOCOMMIT statements, prepared — racing row must APPEAR | 13 | 13 OK |
| Two statements in one prepared `BEGIN`, prepared — racing row must NOT appear | 13 | 13 OK |
| A prepared `ROLLBACK` must discard its buffer | 0 rows | 0 rows OK |

Six checks, zero failures. The first and fourth are the pool's own gate: they
fail exactly when a worker carries a snapshot forward. The second and fifth are
the converse — a pool that took a fresh timestamp for *every* statement would
pass the first and fail these.

One note on the probe rather than the node: a `COM_STMT_EXECUTE` for a
zero-parameter statement ends after the iteration count, with no null bitmap
and no new-params-bound flag. The first version of this probe appended the flag
anyway and **the node correctly rejected it** with `1210 prepared-statement
packet has 1 trailing bytes`. That is the server being right and the probe
being wrong; it is recorded because a lenient server would have accepted it.

### The controlled A/B at `cfd6f963a9`: all four workloads, both ps modes (task #124)

**2026-08-01, port offset 37000, ONE `tiup playground
v9.0.0-beta.2.pre-nightly` cluster** (the same version as the offset-43000 and
offset-44000 runs it is compared against), both engines inside the one run,
`--threads=1 --table-size=1000`, ten seconds per window, `ignored errors: 0` in
all sixteen. Artifacts: the `ab-37000` set. Every line here is keyed to offset
37000 and to that single run.

| Workload | Rust text | Go text | Go faster by | prior (43000) | Rust binary | Go binary | Go faster by | prior (43000) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `oltp_point_select` | 4,122.75 qps | 8,455.57 qps | **2.05x** | 2.24x | 4,288.64 qps | 9,482.25 qps | **2.21x** | 2.27x |
| `oltp_read_only` | 223.64 tps | 436.78 tps | 1.95x | 2.02x | 232.86 tps | 510.76 tps | 2.19x | 2.81x |
| `oltp_write_only` | 568.92 tps | 1,110.67 tps | 1.95x | 2.43x | 429.29 tps | 1,193.62 tps | 2.78x | 3.03x |
| `oltp_read_write` | 140.95 tps | 241.25 tps | 1.71x | 1.74x | 144.69 tps | 301.60 tps | 2.08x | 2.36x |

**Every cell improved.** Read the binary column with the prepared-`BEGIN` fix
in mind, though: those improvements are not the thread pool's, they are the
removal of a per-statement PD round trip. **The text column is the one that
isolates the pool**, because text-mode transaction control was never broken.

Treat `oltp_read_only` as noise unless it moves by more than Go's own spread —
Go's `oltp_read_only` text has now measured 249.45, 430.44 and 436.78 tps
across runs. `oltp_write_only` text also has a wide own-spread: the identical
code measured 2.00x at offset 44000 and 2.43x at offset 43000, so today's 1.95x
is only just outside that band and should not be read as a 20% win.

### Did the 17.5 µs move tps? Yes, by about the predicted amount (task #124)

The prediction was falsifiable and it is worth stating why the raw tps is the
wrong instrument for it: cross-run absolutes on this machine are not
comparable, and the same read code has measured 89.15 and 219.91 tps in
consecutive runs.

**Go's own per-statement time is the fixed point that makes the comparison
work.** Across three runs on this machine `oltp_point_select` text has cost Go
117.40, 118.18 and 118.27 µs per statement — under 1% spread. So the Rust
node's *excess over Go in the same run* is a stable instrument where its raw
tps is not:

| Run | Rust µs/stmt | Go µs/stmt | Excess |
| --- | --- | --- | --- |
| `dd97293671`, offset 43000 | 263.32 | 117.40 | 145.92 |
| `28eebaffa1`, offset 44000 | 251.18 | 118.18 | 133.00 |
| **`cfd6f963a9`, offset 37000** | **242.56** | **118.27** | **124.29** |

The excess fell by **8.70 µs** against the nearer prior run and **21.63 µs**
against the farther one. The cluster-free measurement predicted **17.32 µs**
(27.57 -> 10.25 per statement), and the prediction sits inside that bracket.
**The floor reduction reached the wire.**

What it did *not* do is redraw the standing. `oltp_point_select` remains the
worst shape in the table at 2.05x, and 124 µs of excess per statement remains
after the largest identified piece of the floor was removed. The per-statement
floor was real and it was worth removing; it was not the whole of the gap. The
next measurement is where the remaining ~124 µs goes, which has still not been
profiled inside the node.

### The controlled A/B at `dd97293671`: all four workloads, both ps modes

**2026-08-01, port offset 43000, ONE `tiup playground
v9.0.0-beta.2.pre-nightly` cluster**, both engines inside the one run.
Artifacts: the `u115/out-43000` set. Compare the ratios, not the absolutes —
cross-run absolutes on this machine are not comparable.

| Workload | Rust text | Go text | Go faster by | Rust binary | Go binary | Go faster by |
| --- | --- | --- | --- | --- | --- | --- |
| `oltp_point_select` | 3,797.67 qps | 8,518.12 qps | **2.24x** | 4,018.42 qps | 9,132.63 qps | **2.27x** |
| `oltp_read_only` | 212.95 tps | 430.44 tps | 2.02x | 173.31 tps | 487.72 tps | 2.81x |
| `oltp_write_only` | 435.81 tps | 1,060.91 tps | 2.43x | 376.41 tps | 1,142.00 tps | 3.03x |
| `oltp_read_write` | 140.51 tps | 244.67 tps | 1.74x | 118.39 tps | 279.41 tps | 2.36x |

Against the task #114 run (offset 44000) the picture is flat to slightly worse:
`oltp_write_only` text 2.00x -> 2.43x, `oltp_read_only` text 1.30x -> 2.02x,
`oltp_point_select` 2.13x -> 2.24x. **No workload improved from the write-path
`Point_Get` change.** The `oltp_read_only` movement is the least trustworthy
number here — that workload has swung widely between runs on this machine
(Go's own `oltp_read_only` has measured 249.45, 430.44 and 487.72 tps across
three runs) — but nothing in the table supports a claim of improvement.

### The write path: `Point_Get` landed, the gap did not close (task #115)

**2026-08-01, `dd97293671`, port offset 43000, ONE `tiup playground
v9.0.0-beta.2.pre-nightly` cluster** (server
`8.0.11-TiDB-v9.0.0-beta.2.pre-2052-g23bff31318`), both engines inside the one
run, `--threads=1 --table-size=1000`, ten seconds per window, `ignored errors:
0` everywhere. Artifacts: the `u115/out-43000` set. Every line here is keyed to
offset 43000 and to that single run.

`EXPLAIN` confirms the plan changed. All three write shapes now print what the
read path prints for the identical predicate:

```
rust> EXPLAIN UPDATE sbtest.sbtest1 SET c='x' WHERE id=500
  Update_3              N/A    root
  └─Selection_2         1.00   root   eq(test.sbtest1.id, 500)
    └─Point_Get_1       1.00   root   table:sbtest1  handle:500
```

`UPDATE ... SET k=k+1` and `DELETE ... WHERE id=500` print the same
`Point_Get_1 handle:500`, where at `28eebaffa1` all three printed
`TableRangeScan_1 range:[500,500]`.

**The RPC-shape prediction held exactly. The throughput prediction did not.**
Those are two separate claims, both were checked, and they disagree:

| Statement shape | Rust `kv_scan` before | **Rust `kv_scan` now** | Rust `kv_get` now | Go `kv_scan` | Go `kv_get` |
| --- | --- | --- | --- | --- | --- |
| `UPDATE ... k=k+1` | 0.97 | **0.04** | 2.91 | 0.02 | 1.03 |
| `UPDATE ... c=?` | 1.02 | **0.04** | 2.94 | 0.01 | 1.07 |
| `DELETE` | 0.99 | **0.01** | 1.03 | 0.01 | 0.98 |
| `INSERT` (control) | 0.05 | **0.04** | 2.12 | 0.02 | 0.04 |

The scans are gone and the gets replaced them. `INSERT`, the control, did not
move on either instrument — it has no `WHERE` clause, so the point plan had
nothing to narrow for it.

**And the tps did not follow.** Within-run Rust-vs-Go ratios, before at offset
44000 (`28eebaffa1`) and now at offset 43000 (`dd97293671`):

| Statement shape | Rust before | Rust now | Go now | Gap before | **Gap now** |
| --- | --- | --- | --- | --- | --- |
| `UPDATE ... SET k=k+1 WHERE id=?` | 1,068.46 | 1,013.60 | 2,339.50 | 2.11x | **2.31x** |
| `UPDATE ... SET c=? WHERE id=?` | 1,056.71 | 1,023.76 | 3,037.89 | 2.90x | **2.97x** |
| `DELETE WHERE id=?` | 3,318.28 | 3,415.24 | 6,260.38 | 1.90x | **1.83x** |
| `INSERT` (control) | 928.15 | 964.18 | 2,728.10 | 2.85x | **2.83x** |

Two shapes got marginally worse, one marginally better, none by more than the
run-to-run spread. Go's own numbers barely moved between the two runs
(2,254.70 -> 2,339.50, 3,069.48 -> 3,037.89, 6,310.50 -> 6,260.38, 2,646.37 ->
2,728.10), which is what makes the Rust column's *lack* of movement readable
rather than noise.

**What this rules out.** Converting one `kv_scan` into one `kv_get` per
single-row write was worth nothing measurable. The remaining ~2x on the write
shapes is therefore **not** RPC shape and not scan amplification — both have now
been driven to Go's own levels with no effect. The write gap is made of the
same thing the read gap is made of; see task #117 below, which measures it
directly.

**One residual the counters do name**, offered as an observation and not as a
cause, since the last shape-level fix bought nothing: the two `UPDATE`s issue
**2.9 `kv_get` per single-row statement where Go issues 1.0**, and `INSERT`
issues 2.1 where Go issues 0.04. `DELETE` is the one shape at parity (1.03 vs
0.98) and also the one with the smallest gap (1.83x). That correlation is
suggestive, but the task #115 result above is precisely a case of a suggestive
RPC-count correlation that did not survive being fixed, so it should be
measured before it is believed.

### Where `oltp_point_select` actually goes (task #117)

Same run, same cluster, same offset 43000, plus a second confirming run at the
same offset (`u115/out-43000-c`) that reproduced every figure below.

Both engines plan `Point_Get` for this workload, so the question was never the
access path. Three candidates were measured and **all three are ruled out**:

| Candidate | Measurement | Verdict |
| --- | --- | --- |
| Extra storage round trips | `kv_get` per statement: **Rust 0.99-1.10, Go 0.91-1.10** | **Ruled out** — identical, one get per statement on both |
| Result-set encoding | `SELECT c` (CHAR(120)) vs `SELECT k` (INTEGER), same key, same path: Rust 3,902.48 vs 3,873.80 qps; Go 8,451.83 vs 8,361.53 | **Ruled out** — dropping 120 bytes of result changes nothing on either engine (2.17x vs 2.16x) |
| Snapshot/timestamp acquisition | PD `pd_server_handle_tso_duration_seconds_count` per statement: **Rust 1.00, Go 0.002** | Real difference, but **not the cause** — see below |

**The floor is what carries it.** `SELECT 1` — no table, no storage, no
snapshot, one tiny column — is already **2.59x** (Rust 7,134.26 qps, Go
18,466.63; reproduced at 7,213.42 vs 18,648.35 in the first run). The full
`oltp_point_select` gap is **2.17x**, i.e. *smaller* than the floor. Per
statement at one thread:

| Statement | Go | Rust | Rust excess |
| --- | --- | --- | --- |
| `SELECT 1` (per-statement floor) | 54.15 µs | 140.17 µs | **86.02 µs** |
| `SELECT c ... WHERE id=?` (stock) | 117.00 µs | 258.59 µs | **141.59 µs** |
| the point read alone (difference) | 62.85 µs | 118.42 µs | 55.57 µs |

**About 61% of Rust's per-statement excess on `oltp_point_select` is present
before the statement touches a table at all.** The remaining ~39% is the point
read itself, and Rust's marginal cost for that read is ~1.9x Go's — including
the one PD TSO round trip Rust takes and Go does not.

**Why this matters out of proportion to `oltp_point_select`.** An 86 µs
per-statement constant is charged to every statement of every workload, and it
is the same constant whether the statement does one `kv_get` or a whole
transaction. That is why it dominates the workload that does the least engine
work, and why it is diluted — not absent — in the others. It is also the
simplest explanation available for the task #115 result above: the write shapes
did not speed up when their RPCs changed shape because their RPCs were never
the binding constraint.

**The TSO difference, named precisely.** Go skips the timestamp entirely for
autocommit point reads. `pkg/sessiontxn/isolation/optimistic.go`:

```go
// AdviseOptimizeWithPlan providers optimization according to the plan
// It will use MaxTS as the startTS in autocommit txn for some plans.
	ok = plannercore.IsPointGetWithPKOrUniqueKeyByAutoCommit(p.sctx.GetSessionVars(), realPlan)
	if ok {
		...
		if err = p.forcePrepareConstStartTS(math.MaxUint64); err != nil {
```

`GetStmtReadTS` then returns `math.MaxUint64` without activating the
transaction. The measured Go TSO rate for `oltp_point_select` (0.002 per
statement) matches that exactly, and Go's rate for `SELECT 1` is 1.00 — the
optimization is plan-gated, so it fires for the point get and not for the
constant select. This is a real, cheap, identified difference, but note the
arithmetic: it lives inside the 55.57 µs marginal term, not the 86.02 µs floor,
so it cannot be worth more than ~39% of this workload's gap.

**A separate finding from the same counters: binary-prepared mode takes a
timestamp per statement instead of per transaction.** In text mode Rust takes
~1.0 TSO per *transaction*; under `--db-ps-mode=auto` it takes one per
*statement*:

| Workload | Rust TSO/txn, text | Rust TSO/txn, binary | Go TSO/txn | Rust tps text | Rust tps binary |
| --- | --- | --- | --- | --- | --- |
| `oltp_read_only` (14 stmt/txn) | 1.08 | **16.11** | 1.04 | 212.95 | 173.31 |
| `oltp_write_only` (6 stmt/txn) | 1.04 | **7.04** | 2.02 | 435.81 | 376.41 |
| `oltp_read_write` (20 stmt/txn) | 1.13 | **21.15** | 2.06 | 140.51 | 118.39 |

**Superseded 2026-08-01 at `cfd6f963a9`, port offset 37000** (same PD
`pd_server_handle_tso_duration_seconds_count` scrape, before and after each
ten-second window, one `v9.0.0-beta.2.pre-nightly` cluster). The table above
records the defect; this one records the fix, and **the binary column has
collapsed onto the text column on all three workloads**:

| Workload | Rust TSO/txn, text | Rust TSO/txn, binary | was, binary | Go TSO/txn, text |
| --- | --- | --- | --- | --- |
| `oltp_read_only` (14 stmt/txn) | 1.0782 | **1.0739** | 16.11 | 1.0435 |
| `oltp_write_only` (6 stmt/txn) | 1.0315 | **1.0408** | 7.04 | 2.0152 |
| `oltp_read_write` (20 stmt/txn) | 1.1496 | **1.1222** | 21.15 | 2.0721 |

A prepared transaction now takes one timestamp for the whole transaction, as
the text arm always did. The throughput pattern this explained has reversed
with it: binary prepared is now **faster** than text on `oltp_point_select`
(4,288.64 vs 4,122.75 qps), `oltp_read_only` (232.86 vs 223.64 tps) and
`oltp_read_write` (144.69 vs 140.95 tps). It remains slower on
`oltp_write_only` (429.29 vs 568.92 tps), which the TSO rate no longer explains
and which is not yet accounted for.

This is the mechanism behind a pattern visible in every table in this document
and never explained: **binary prepared is consistently slower than text on the
Rust node, while it is consistently faster on Go.** A per-statement PD round
trip inside a multi-statement transaction is also a correctness-adjacent smell,
not only a throughput one, and it is worth its own investigation.

*(That investigation is task #120, and the smell was the story: the prepared
path never called `control_transaction`, so a prepared `BEGIN` opened no
transaction at all. The numbers above stand as measured; the defect behind them
is fixed. See open-items entry 13.)*

### The controlled A/B (task #114): after the write-path point plan

**2026-08-01, `28eebaffa1`, port offset 44000, ONE `tiup playground
v9.0.0-beta.2.pre-nightly` cluster** (server
`8.0.11-TiDB-v9.0.0-beta.2.pre-2052-g23bff31318`) — **both engines on the same
version this time**, which the previous baseline did not do. Rust node in
`--cluster-session` against the same PD, sysbench alternating between the two
SQL ports inside the one run, same TiKV, same laptop, ten seconds each,
`--threads=1 --table-size=1000`, `ignored errors: 0` in all twenty-four
windows. Artifacts: the `probe-44000-u114` set. Every line below is keyed to
offset 44000 and to that single run.

| Workload | Rust text | Go text | Go faster by | Rust binary | Go binary | Go faster by |
| --- | --- | --- | --- | --- | --- | --- |
| `oltp_point_select` | 3,981.20 qps | 8,461.32 qps | 2.13x | 4,176.82 qps | 9,124.82 qps | 2.18x |
| `oltp_read_only` | 191.32 tps | 249.45 tps | 1.30x | 157.92 tps | 270.48 tps | 1.71x |
| `oltp_write_only` | 550.18 tps | 1,102.89 tps | **2.00x** | 378.48 tps | 1,189.24 tps | **3.14x** |
| `oltp_read_write` | 143.20 tps | 250.99 tps | 1.75x | 120.90 tps | 301.77 tps | 2.50x |

**The write gap went from 9.97x to 2.00x in text mode and 12.04x to 3.14x
binary**, measured the same way in the same kind of run. `oltp_write_only` is
no longer the worst shape in the table; `oltp_point_select` is.

### The write path: per-statement breakdown after the fix (task #114)

Same run, same instruments as the task #112 breakdown below: each of
`oltp_write_only`'s four shapes measured alone against both engines, with
TiKV's `tikv_grpc_msg_duration_seconds_count` scraped before and after each
ten-second window.

**The prediction task #112 made was falsifiable and it held on both
instruments** — the scans fell AND the throughput followed. Those are two
separate claims and both were checked, because a scan count that falls without
the tps moving would have meant scan amplification was never the dominant cost.

| Statement shape | Rust tps before | Rust tps after | Go tps (this run) | Gap before | **Gap after** |
| --- | --- | --- | --- | --- | --- |
| `UPDATE ... SET k=k+1 WHERE id=?` | 256.58 | **1,068.46** | 2,254.70 | 8.9x | **2.11x** |
| `UPDATE ... SET c=? WHERE id=?` | 254.70 | **1,056.71** | 3,069.48 | 12.1x | **2.90x** |
| `DELETE WHERE id=?` | 347.11 | **3,318.28** | 6,310.50 | 18.2x | **1.90x** |
| `INSERT` (control) | 953.61 | 928.15 | 2,646.37 | 2.9x | 2.85x |

**`INSERT` is the control and it did not move**: 2.9x -> 2.85x, a difference
smaller than the run-to-run spread. It has no `WHERE` clause, so the write
lowering had nothing to narrow for it. Go's own per-shape numbers also barely
moved between the two runs (2,277 -> 2,255, 3,088 -> 3,069, 6,327 -> 6,311,
2,746 -> 2,646), which is what makes the Rust column's movement readable rather
than noise.

TiKV `kv_scan` RPCs per statement, the mechanism, from the counter diffs:

| Statement shape | Rust `kv_scan` before | **Rust `kv_scan` after** | Go `kv_scan` |
| --- | --- | --- | --- |
| `UPDATE ... k=k+1` | 5.57 | **0.97** | 0.06 |
| `UPDATE ... c=?` | 6.08 | **1.02** | 0.02 |
| `DELETE` | 6.54 | **0.99** | 0.02 |
| `INSERT` (control) | 0.05 | **0.05** | 0.01 |

Five to seven scans per single-row write became one, and `INSERT` stayed at the
background rate. `EXPLAIN`, on the same connection in the same run, names the
change:

```
rust> EXPLAIN UPDATE sbtest.sbtest1 SET k=k+1 WHERE id=500
  Update_3                N/A    root
  └─Selection_2           1.00   root   eq(test.sbtest1.id, 500)
    └─TableRangeScan_1    1.00   root   table:sbtest1  range:[500,500]
```

`UPDATE ... SET c='x'` and `DELETE ... WHERE id=500` print the same
`range:[500,500]`, where all three printed `TableFullScan_1 10000.00` before.

**The residual, and it is a specific one.** The write path lands on
`TableRangeScan` with a one-key range, not on `Point_Get` — the read path for
the identical predicate still prints `Point_Get_1 handle:500`. That is exactly
why Rust issues **1.0 `kv_scan` per write where Go issues ~0.02 plus a
`kv_get`**: a degenerate one-key range is still executed as a scan RPC rather
than a point get. Turning that last scan into a `kv_get` is the next lever on
the write path, and it is worth roughly the remaining 2x, not another 9x. The
non-index `UPDATE` is the furthest off (2.90x) and also the shape where Go
spends the least (0.97 `kv_prewrite`, 0.01 `kv_commit` — Go is doing something
cheaper at commit there that this node is not).

### Superseded: the controlled A/B at `de899c65d1`, before the write-path fix

The uncontrolled comparison above mixed `v8.5.6` (Rust) with
`v9.0.0-beta.2.pre-nightly` (Go). Both versions are installed locally, so the
control was cheap: **2026-08-01, `de899c65d1`, port offset 44000, ONE `tiup
playground v9.0.0-beta.2.pre-nightly` cluster** (server version
`8.0.11-TiDB-v9.0.0-beta.2.pre-2052-g23bff31318`), the Rust node in
`--cluster-session` against the same PD, sysbench alternating between the two
SQL ports within the same run, same TiKV, same laptop, same ten seconds of
workload each. The Rust node prepares and runs unmodified on `v9.0.0-beta.2`
as well as on `v8.5.6`. Artifacts: the `probe-44000` set.

| Workload | Rust text | Go text | Go faster by | Rust binary | Go binary | Go faster by |
| --- | --- | --- | --- | --- | --- | --- |
| `oltp_point_select` | 3,993.94 qps | 8,437.69 qps | 2.11x | 4,171.26 qps | 9,207.25 qps | 2.21x |
| `oltp_read_only` | 196.98 tps | 266.19 tps | **1.35x** | 162.74 tps | 278.88 tps | 1.71x |
| `oltp_write_only` | 113.10 tps | 1,127.14 tps | **9.97x** | 101.61 tps | 1,223.05 tps | 12.04x |
| `oltp_read_write` | 69.90 tps | 217.01 tps | 3.10x | 54.74 tps | 286.87 tps | 5.24x |

Two things this controls for, and one it does not:

* **The 9.8x write gap is real and not a version artifact.** On one cluster it
  measures 9.97x in text mode and 12.04x binary. Nothing about the earlier
  mixed-version comparison was inflating it.
* **The read gap is smaller than the uncontrolled figure said.** 4.7x becomes
  1.35x here. Beware of concluding the read path improved by that much: Go's
  own `oltp_read_only` fell from 419.71 to 266.19 tps between the standalone
  baseline and this run, in which sixteen back-to-back benchmarks share the
  laptop. **Absolute numbers move a lot run to run; only ratios measured inside
  a single run are trustworthy, and this whole table is one run.**
* Not controlled: still one thread, 1,000 rows, one laptop hosting both the
  cluster and the load generator.

### Superseded: the write path before the fix, per-statement (task #112)

Conditions: 2026-08-01, `de899c65d1`, port offset 44000, one `tiup playground
v9.0.0-beta.2.pre-nightly` cluster, artifacts the `probe-44000` set. Kept
because it is the diagnosis the fix was built from, and because its prediction
is the one task #114 confirmed.

`oltp_write_only` issues four statement shapes. sysbench ships each as its own
workload, so each was measured alone, on the same cluster, against both
engines, with **TiKV's `tikv_grpc_msg_duration_seconds_count` scraped before
and after each ten-second window** — a wire-level round-trip count per
statement, not an inference.

| Statement shape (workload) | Rust tps | Rust avg ms | Go tps | Go avg ms | Go faster by |
| --- | --- | --- | --- | --- | --- |
| `UPDATE ... SET k=k+1 WHERE id=?` (`oltp_update_index`) | 256.58 | 3.90 | 2,277.24 | 0.44 | 8.9x |
| `UPDATE ... SET c=? WHERE id=?` (`oltp_update_non_index`) | 254.70 | 3.93 | 3,087.53 | 0.32 | 12.1x |
| `DELETE WHERE id=?` (`oltp_delete`) | 347.11 | 2.88 | 6,327.26 | 0.16 | 18.2x |
| `INSERT` (`oltp_insert`) | 953.61 | 1.05 | 2,746.48 | 0.36 | **2.9x** |

TiKV RPCs per statement, from the counter diffs over the same windows:

| Statement shape | Rust `kv_scan` | Rust `kv_get` | Rust `kv_prewrite` | Go `kv_scan` | Go `kv_get` | Go `kv_prewrite` |
| --- | --- | --- | --- | --- | --- | --- |
| `UPDATE ... k=k+1` | **5.57** | 2.47 | 1.67 | 0.09 | 1.04 | 1.83 |
| `UPDATE ... c=?` | **6.08** | 2.23 | 1.94 | 0.02 | 0.94 | 0.94 |
| `DELETE` | **6.54** | 0.72 | 0.66 | 0.01 | 0.93 | 0.02 |
| `INSERT` | **0.05** | 2.02 | 1.89 | 0.02 | 0.04 | 1.98 |

**The one shape that does no scanning is the one that is nearly as fast as Go.**
`INSERT` has no `WHERE` clause, issues 0.05 `kv_scan` per statement (that is the
background rate — 430 calls in ten seconds appears in every window on this
cluster, including idle ones), and is only 2.9x off Go. The three shapes with a
`WHERE id=?` predicate each issue **five to seven `kv_scan` RPCs per single-row
statement** and are 8.9x to 18.2x off. The correlation is complete across the
four shapes.

`EXPLAIN` names the cause, on the same connection, at the same moment:

```
rust> EXPLAIN UPDATE sbtest.sbtest1 SET k=k+1 WHERE id=500
  Update_3              N/A       root
  └─Selection_2         10.00     root   eq(test.sbtest1.id, 500)
    └─TableFullScan_1   10000.00  root   table:sbtest1  stats:pseudo

rust> EXPLAIN SELECT c FROM sbtest.sbtest1 WHERE id=500
  Projection_3          1.00      root   test.sbtest1.c
  └─Selection_2         1.00      root   eq(test.sbtest1.id, 500)
    └─Point_Get_1       1.00      root   table:sbtest1  handle:500
```

**The identical predicate reaches `Point_Get` under `SELECT` and
`TableFullScan` under `UPDATE`.** `DELETE ... WHERE id=500` and
`UPDATE ... SET c='x' WHERE id=500` print the same `TableFullScan_1 10000.00`.
The access-path work that landed for reads did not reach the DML path, and the
`kv_scan` counts are that full scan crossing the wire, five to seven RPCs at a
time, for every single-row write.

**What the numbers rule OUT — each of these was a candidate, and each is now
eliminated by measurement, not by argument:**

* **Per-statement 2PC round trips are not the cause.** Rust issues FEWER
  commit-protocol RPCs than Go, not more: 1.67 `kv_prewrite` per indexed update
  against Go's 1.83, and 0.66 against Go's 0.02 for `DELETE`. Go also spends
  2.53 `kv_pessimistic_lock` per `oltp_write_only` transaction where Rust
  spends ~0. Whatever Rust is doing wrong, it is not paying more for commit.
* **"Each write opens its own transaction" does not distinguish the shapes.**
  All four shapes are autocommit single statements, yet `INSERT` is 2.9x and
  `DELETE` is 18.2x. Transaction setup cost cannot produce a 6x spread among
  statements that all set up one transaction.
* **Index-entry maintenance read-modify-write is not the cause.**
  `UPDATE ... SET c=?` touches no index — `c` is in no key — and scans slightly
  MORE than `UPDATE ... SET k=k+1`, which maintains `k_1`. The two are within
  1% of each other in throughput (254.70 vs 256.58 tps).
* **The write path does NOT take the point-get plan.** This is the one
  candidate the measurements support, and both instruments agree on it:
  `EXPLAIN` prints `TableFullScan` and TiKV counts the scans.

Follow-on work is therefore scoped: extend the clustered-handle access-path
construction from `SELECT` to `UPDATE`/`DELETE`. The prediction this makes, and
which the next ladder run can falsify, is that `kv_scan` per single-row write
falls to the background rate and the three predicate shapes converge toward
`INSERT`'s 2.9x.

**That prediction was tested at `28eebaffa1` and held**, with one correction:
`kv_scan` fell to **one** per write rather than to the background rate, because
the write path lands on a one-key `TableRangeScan` and not on `Point_Get`. The
three predicate shapes did converge on `INSERT` — 2.11x, 2.90x and 1.90x
against its 2.85x. See "The write path: per-statement breakdown after the fix".

A note on the artifacts: rung C of that probe printed post-run row counts of
`10226` (Rust) and `27972` (Go), which are NOT a correctness disagreement —
`oltp_insert` and `oltp_delete` mutate each server's own table independently
and Go, being faster, performed several times more of both. The correctness
gate is rung 5 of the ladder, which matched exactly.

**Where the remaining read gap goes** (written against the 4.7x uncontrolled
figure; the controlled A/B measures 1.35x, but the mechanism below is unchanged
and still unimplemented). The 4.7x on `oltp_read_only` is the
predicted residual, not a failure of the range fix: aggregate pushdown is still
absent, so Go runs a cop-side `StreamAgg` for `SUM(k)` while this node drags
the rows to the root and aggregates there. The write gap (9.8x) is larger and
is a separate matter — it is not explained by the read path. **It has since
been analysed by measurement: see "The write path: per-statement breakdown".
DML by primary key plans as `TableFullScan`.**

### Regression found by this run, and closed: error 2014 under binary prepared statements

**Resolved. Re-measured 2026-08-01 at `de899c65d1`, port offset 45000: both
cells ran, `oltp_read_only` binary prepared at 178.82 tps and
`oltp_read_write` binary prepared at 52.44 tps, `ignored errors: 0`, and no
occurrence of `2014` in any log of that run. Rung 5's checksum matched Go
exactly (`1000 500500 501715 1 1000`), so the protocol fix changed no row
set.** The account below is the diagnosis as it stood when the regression was
open, kept because the chain is the useful part.

Two cells that previously held numbers then failed. `oltp_read_only` and
`oltp_read_write` under the **default** `--db-ps-mode` (binary prepared
statements) both abort with the same error, on the same statement:

```
FATAL: mysql_stmt_execute() returned error 2014 (Commands out of sync;
you can't run this command now)
for query 'SELECT c FROM sbtest1 WHERE id BETWEEN ? AND ?'
   at oltp_common.lua:432
```

This is a protocol-framing defect, not a SQL one: "commands out of sync" means
the client found the server's packet stream in a state it did not expect, so
the binary-protocol resultset for that statement is malformed or mis-sequenced.

What narrows it:

* It is **specific to the parameterised range select**. `oltp_point_select`
  under the same binary protocol runs fine (3,372.71 qps), as does
  `oltp_write_only` (93.65 tps). Only the two workloads containing
  `SELECT c ... WHERE id BETWEEN ? AND ?` fail.
* The identical statement in **text** mode succeeds — that is the 89.15 tps
  cell.
* It is **new**. The pre-range-fix run measured both these cells (11.35 and
  7.39 tps), so the binary path served this statement before.
* The failing statement is exactly the shape the clustered-handle range fix
  changed, and the fix altered which executor serves it.

**Root-caused and fixed on this branch; the two cells are not yet re-measured.**
The chain, end to end:

1. A prepared query reports its result columns at PREPARE by planning with
   every marker bound to NULL. For this statement that is
   `id BETWEEN NULL AND NULL`, which the new range builder resolves to an
   EMPTY handle-range list — `EXPLAIN` prints `TableRangeScan ... range:` with
   nothing in it, estimating 0 rows. That is correct: no handle qualifies.
2. The byte cursor states "read nothing" exactly, by opening no iterator. The
   coprocessor request cannot: its `Ranges` list is what the transport turns
   into region tasks, so an empty one is a malformed request, and
   `tidb_distsql`'s `metadata_region_ranges` refuses it as `missing_ranges`
   before any RPC. Only a node WITH a coprocessor — the cluster session, not
   the in-process one — reaches this, which is why no unit test saw it.
3. `prepare_general` swallows a failed probe into "no result columns". So the
   PREPARE answered zero columns.
4. A MySQL client frames the `COM_STMT_EXECUTE` answer against that count. The
   execute then sent a real one-column result set, leaving the client a whole
   result set behind — and its next command reported `2014`.

Text mode never asks the question (no PREPARE metadata), `oltp_point_select`
takes the point-read path whose columns come from the catalog, and
`oltp_write_only` prepares no query, which is exactly the three-way split the
measurements showed.

The fix is in `tidb_executor::kv_table::table_scan`: a scan whose ranges cover
no record refuses the pushdown and is served by the local cursor, which returns
no row without a request — what Go does one level higher by planning a
`TableDual`. The cluster-free reproduction is
`tidb_executor::remote_scan`'s `an_empty_handle_range_reads_nothing_instead_of_a_rangeless_request`
(its fake coprocessor refuses a rangeless request the way the transport does),
and the wire contract the client actually reads is pinned by
`prepared_handle_range_frames_its_binary_result_set` in
`tidb-server/tests/pipeline_mysql_client_source.rs`, which asserts the prepare's
column count, the execute header that repeats it, and the packet run behind it.

**The ladder run that was owed has been made.** `rust/scripts/run-sysbench-ladder.sh`
at `de899c65d1`, port offset 45000: `oltp_read_only-ps-auto.log` shows
`transactions: 1789 (178.82 per sec.)` and `ignored errors: 0`, and
`oltp_read_write-ps-auto.log` shows `525 (52.44 per sec.)`. The fix holds on a
real cluster, which is the only place the defect ever appeared.

## The new frontier: pushdown (range extraction is FIXED)

**Status update, 2026-08-01, `de899c65d1`.** Defect 1 below is fixed **for
`SELECT` only** — `UPDATE`/`DELETE ... WHERE id=?` still print
`TableFullScan_1 10000.00` and pay five to seven `kv_scan` RPCs per single-row
write, which the per-statement breakdown above measures as the write-path gap.
Read the rest of this section as the read path's record.

**Status update, 2026-08-01, `8eacf363e5`.** Defects 1 and 3 below are fixed.
The planner now builds `TableRangeScan ... range:[100,199]` with a 99-row
estimate where it used to print `TableFullScan 10000.00`, and `SUM(k)` no
longer picks a full scan of an index its predicate never mentions. The
throughput consequence is measured above: `oltp_read_only` 11.70 -> 89.15 tps.
**Defect 2, coprocessor pushdown, remains** — every Rust plan node still
reports `root`, so Go's cop-side `StreamAgg` for `SUM(k)` has no counterpart
here and the rows still cross the wire to be aggregated. That is the 4.7x that
is left against the Go baseline. The diagnosis below is retained as the record
of what was wrong and how it was found.

With the three blockers gone, the first honest limit is the planner. `EXPLAIN`
of the four `oltp_read_only` range queries, side by side with the Go server on
the same cluster and the same table, names it exactly (measured 2026-08-01,
`4634a071e7`, port offset 44000):

```
SELECT c FROM sbtest1 WHERE id BETWEEN 100 AND 199
  rust: Projection_3 8000.00 root
        └─Selection_2 8000.00 root  `id` BETWEEN 100 AND 199
          └─TableFullScan_1 10000.00 root  table:sbtest1
  go:   TableReader_9 99.00 root  data:Projection_5
        └─Projection_5 99.00 cop[tikv]
          └─TableRangeScan_8 99.00 cop[tikv]  range:[100,199]
```

Three defects, in descending order of cost:

1. **No range extraction on the clustered primary key.** `id BETWEEN 100 AND
   199` becomes a `TableFullScan` plus a filter, so the node reads all 1,000
   rows to answer a 99-row range. Go builds `TableRangeScan range:[100,199]`.
2. **Nothing is pushed to the coprocessor.** Every Rust plan node reports
   `root`; Go runs the selection, the projection and the aggregate at
   `cop[tikv]`. So the rows do not merely get read, they get shipped.
3. **The secondary index is chosen for a predicate that does not mention it** —
   this is what made the index a regression rather than a win:

```
SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN 100 AND 199
  rust: HashAgg_3 1.00 root  funcs:sum(k)
        └─Selection_2 8000.00 root  `id` BETWEEN 100 AND 199
          └─IndexFullScan_1 10000.00 root  index:k_1(k)
  go:   StreamAgg_17 → TableReader_18 → StreamAgg_9 cop[tikv]
        └─TableRangeScan_16 99.00 cop[tikv]  range:[100,199]
```

Before `k_1` existed the only path was a table scan; now the optimizer prefers
a **full scan of `k_1`** for a predicate on `id`, and still filters at `root`.
That single query is one of the four in every `oltp_read_only` transaction,
which is where the seven-fold slowdown comes from.

The row estimates show why the cost model cannot prefer the range: the Rust
side estimates `8000.00` rows for a range that returns 99, because with no
range extraction there is no range to estimate. `stats:pseudo` on both sides,
so this is not a statistics gap — it is the access-path construction.

A fourth, cosmetic: `WHERE id=500` reaches `Point_Get` on both engines, but the
Rust plan wraps it in a redundant `Selection` + `Projection` that Go does not
emit. Consistent with point-select throughput being the one workload that did
not regress.

**None of this is a correctness defect.** Every result matched Go byte for
byte, rung 5 and rung 7 both. Reproduce with the ladder plus an `EXPLAIN` of
the statements above; the table is exactly what stock `prepare` builds:

```
CREATE TABLE `sbtest1` (
  `id` int NOT NULL AUTO_INCREMENT,
  `k` int NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `k_1` (`k`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

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
`SUM(k)` queries keep the index they exist to exercise. Measuring that on the
merged tip turned up the sting: the planner then picks a FULL scan of that
index for a predicate on `id`, so keeping the index costs throughput rather
than earning it. Correctness is unaffected; see "The new frontier".

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
backfill writes entries through the ordinary 2PC and needs no local disk. This
reproduced again on the 2026-08-01 merged-tip run; the threshold that causes it
is analysed under "The Go control rung".

**Still true on the merged tip (2026-08-01, `4634a071e7`):** rung 7's
`CREATE INDEX` / `ADMIN CHECK TABLE` / `USE INDEX` vs `IGNORE INDEX` /
`DROP INDEX` / re-`ADMIN CHECK` sequence all passed, and the index is now also
built by stock `sysbench prepare` rather than only by hand. Rung 7 totals moved
again, from 21 accepted / 3 refused to **24 accepted / 0 refused**, the three
former refusals being blocker 3.

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
3. ~~`--auto-inc=off`~~ — done; the persistent allocator under blocker 3 landed
   and `prepare` runs with sysbench's default schema.
4. ~~A ladder run on a tree carrying all three fixes.~~ Done, 2026-08-01 at
   `4634a071e7`: `prepare` completes, rung 5's Rust-vs-Go checksum ran and
   matched, rung 7 is 24/0.
5. ~~Range extraction on the clustered primary key~~ — done, `8eacf363e5`,
   worth 7.6x on `oltp_read_only` (11.70 -> 89.15 tps).
6. ~~A Go baseline on this machine.~~ Done, 2026-08-01, via a
   `v9.0.0-beta.2.pre-nightly` playground; ~93Gi free was never needed.
7. **Coprocessor and aggregate pushdown.** Now the largest known read-side
   gap: 4.7x against Go on `oltp_read_only` — see "The new frontier".
8. **The error-2014 regression** on `SELECT c ... WHERE id BETWEEN ? AND ?`
   under binary prepared statements. This is a correctness/protocol defect and
   outranks the throughput work.
9. ~~**The write gap.**~~ Largely closed, 2026-08-01 at `28eebaffa1`. The write
   lowering never enumerated access paths, so every `WHERE id=?` write scanned
   the whole record range; giving it the read side's handle ranges took the
   controlled-A/B `oltp_write_only` gap from **9.97x to 2.00x** in text mode and
   `kv_scan` per single-row write from 5.6-6.5 to ~1.0. What remains of it is
   item 10.
10. ~~**`Point_Get` for writes.**~~ Done, 2026-08-01 at `dd97293671`, and
    **it was worth nothing measurable.** The plan changed, `kv_scan` per
    single-row write fell from ~1.0 to ~0.03 and `kv_get` rose to match — and
    the within-run gap stayed at 2.31x / 2.97x / 1.83x against 2.11x / 2.90x /
    1.90x. The estimate that it was "worth roughly the remaining 2x" was
    **wrong**, and the reason it was wrong is item 12.
11. ~~**`oltp_point_select` is the widest gap.**~~ Measured, 2026-08-01. It is
    not an access-path, round-trip, or encoding problem: `kv_get` per statement
    is 1.0 on both engines, and projecting an `INTEGER` instead of a
    `CHAR(120)` changes nothing. It is item 12.
12. **The per-statement floor: ~86 µs that Rust pays before a statement touches
    a table.** `SELECT 1` measures 2.59x against Go, which is *wider* than
    `oltp_point_select`'s own 2.17x. This constant is charged to every
    statement of every workload — it dominates the workloads that do the least
    engine work and is diluted, never absent, in the rest. **It is now the
    single largest known item on this list**, it explains why item 10 bought
    nothing, and unlike the access-path work its fix lifts all four workloads.
    Where the 86 µs goes inside the Rust node has not yet been profiled; that
    is the next measurement, not the next guess.
    **Partly addressed 2026-08-01 at `cfd6f963a9`, and still open.** The
    largest identified piece of the floor — one OS thread spawned and joined
    per statement, 27.57 µs cluster-free — was replaced by a parked worker at
    10.25 µs, and **the saving reached the wire**: `oltp_point_select` text
    excess over Go fell from 133.00 to 124.29 µs per statement in a run where
    Go's own per-statement time was unchanged (118.18 -> 118.27 µs). That is
    the predicted 17.32 µs within run-to-run bracket. But **124 µs of excess
    per statement remains**, `oltp_point_select` is still the worst shape at
    2.05x, and where *that* goes is still unprofiled. This item stays open, and
    it stays the largest one.
13. ~~**Binary-prepared execution takes a TSO per statement, not per
    transaction**~~ (16.11 per `oltp_read_only` transaction against 1.08 in
    text mode) — **diagnosed and fixed, task #120.** The "correctness-adjacent"
    reading was the right one and it was worse than the throughput: nothing on
    the `COM_STMT_PREPARE`/`COM_STMT_EXECUTE` path called `control_transaction`,
    so a prepared `BEGIN` never opened the connection's transaction and every
    statement of the transaction read at its own fresh timestamp — no
    repeatable read and no conflict detection, with a prepared `ROLLBACK`
    publishing the buffer it was asked to discard. Transaction control is now
    routed the way the text arm routes it; see
    `cluster_session_node::tests::prepared_transactions`. ~~The TSO rate itself
    has not been re-measured on a cluster since the fix.~~ **Re-measured
    2026-08-01 at `cfd6f963a9`, port offset 37000: 16.11 -> 1.07, 7.04 -> 1.04,
    21.15 -> 1.12, each landing on its own text rate.** The repeatable-read and
    rollback semantics were checked on the same cluster over the prepared
    protocol and hold; see "Snapshot semantics under the pinned thread pool".
    **Closed.**
14. **Autocommit point reads could skip the timestamp entirely,** as Go does
    via `IsPointGetWithPKOrUniqueKeyByAutoCommit` and a `MaxUint64` start ts.
    ~~Rust takes 1.00 TSO per point select where Go takes 0.002.~~ **Partly
    landed and still open, 2026-08-01 at `63acc31852`:** the shortcut works and
    is correct on a real cluster where it is wired — a `--load-table` node
    takes **0** timestamps for 200 autocommit point gets, returns Go's exact
    rows, and keeps both visibility rules — but `--cluster-session`, the mode
    every sysbench run uses, binds its statement snapshot before planning and
    so still takes **1.005** per point select. See "The `MaxUint64` point-get
    shortcut on a real cluster (task #142)". Bounded by the
    arithmetic in task #117 to at most ~39% of that workload's gap, so it ranks
    below item 12.

An unqualified sysbench number is available today in the sense that stock
sysbench runs unmodified end to end. It is not yet a competitive one.

## The Go control rung: the baseline now exists

**Resolved 2026-08-01.** The newer-playground route works and is cheap. A
`tiup playground v9.0.0-beta.2.pre-nightly` (server
`8.0.11-TiDB-v9.0.0-beta.2.pre-2052-g23bff31318`) at port offset 44000 runs
stock `sysbench prepare` to completion — `CREATE INDEX` included, `PRIMARY,k_1`
present, `1000 500500 1 1000` — with **38Gi free of 926Gi**, i.e. about 4%,
far under the 10% the check nominally demands. That confirms the `darwin`
early return is what carries it, and that no disk needed freeing. The
throughput figures are in "The Go baseline" above.

~~The ladder script itself still pins `v8.5.6`, so its own rung 3c still
fails~~ — **fixed 2026-08-01 at `28eebaffa1`**: the version is now
`SYSBENCH_CLUSTER_VERSION`, defaulting to `v9.0.0-beta.2.pre-nightly`, and rung
3c passed on the offset-45000 run. The control is folded back in, so the ladder
and its Go control share one cluster version.

The original diagnosis follows, and remains correct about the mechanism.

The Go control (rung 3c) fails again on the merged-tip run, with the same
`error 8256: Check ingest environment failed: no enough space in
/tmp/tidb/tmp_ddl-47000` at `CREATE INDEX`. Freeing ~52GB before this run did
not clear it, and the reason is that **the check is a percentage of the volume,
not an absolute amount** — `pkg/ddl/ingest/disk_root.go`:

```go
const capacityThreshold = 0.9

func RiskOfDiskFull(available, capacity uint64) bool {
	return float64(available) < (1-capacityThreshold)*float64(capacity)
}
```

`PreCheckUsage` refuses whenever free space is under 10% of capacity. At the
time of this run `df -h` reported **44–49Gi available of 926Gi, i.e. ~5%**, so
the check trips. On this machine a Go baseline needs roughly **93Gi free**, and
no amount of clearing the temp directories helps: `/tmp/tidb/tmp_ddl-*` are all
0 bytes. It was never about their contents.

So the disk theory is confirmed as the mechanism and refuted as stated: the
requirement is proportional to a 926Gi volume, not to the tiny index being
built.

Two further notes, neither of which changes the above:

* On current `master` this would not error at all — `PreCheckUsage` gained an
  explicit `runtime.GOOS == "darwin"` early return in `c619031356` ("ddl:
  ignore ingest's disk check in the darwin", #60894, 2025-04-29). The
  playground runs the released `v8.5.6` binary, which predates it. A newer
  playground version is therefore a second way to get a Go baseline.
* Go could not create the index and the Rust node could, on the same volume in
  the same run. That is not a correctness claim about either — it is a
  difference in where the work stages. Go's add-index reorg goes through the
  local ingest/lightning temp directory; this node's backfill writes entries
  through the ordinary 2PC and needs no local disk.

~~**Consequence for this document: there is still no Go throughput baseline.**~~
**Superseded 2026-08-01** — the baseline was taken via the newer playground, as
described at the head of this section. The rung-6 table is now compared against
real Go numbers from this machine, with the version caveat stated there.
