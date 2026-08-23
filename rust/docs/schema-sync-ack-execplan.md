# The cluster-session node acknowledges schema versions to Go's DDL owner

Status: in progress (2026-08-23). Keep `Progress`, `Surprises & Discoveries`,
and `Decision Log` current while implementing.

## Purpose / Big Picture

The node registers itself under `/tidb/server/info/<uuid>` so peers can see
it — and Go's DDL owner therefore WAITS for it. `WaitVersionSynced`
(`pkg/ddl/schemaver/syncer.go`) builds its wait set from the registered
server infos and polls `/tidb/ddl/all_schema_by_job_versions/<jobID>/<id>`
until every node reports the job's version. This node never writes that key,
so ANY DDL issued through a Go tidb-server on a shared cluster blocks
forever. Receipt: the sysbench ladder's rung 3c (`CREATE DATABASE sbtest_go`
against the Go control server) hung 17 minutes while Go logged
`"syncer check all versions, someone is not synced"` naming this node's ip,
port, and server-info id, once per second.

The fix is Go's own non-owner contract: after loading schema version V, read
`mysql.tidb_mdl_info`, and for each job whose version is at most V — and
whose old schema no live local work still uses — PUT the ack key.

## Go contract being ported

- `pkg/infoschema/issyncer/syncer.go` `refreshMDLCheckTableInfo`: after each
  reload, `SELECT job_id, version, table_ids FROM mysql.tidb_mdl_info WHERE
  job_id >= min_job_id AND version <= <loaded version>`.
- `MDLCheckLoop`: `CheckOldRunningTxn` removes jobs whose tables are still
  used by sessions on an older schema; the survivors are acked via
  `UpdateSelfVersion(jobID, ver)`, with a per-job cache so an ack is sent
  once.
- `pkg/ddl/schemaver/syncer.go` `UpdateSelfVersion` (MDL on, the default):
  PUT `/tidb/ddl/all_schema_by_job_versions/<jobID>/<ddlID>` = version
  (`PutKVToEtcdMono`; the mono guard only defends the key against the same
  node's own out-of-order writers, and this port has exactly one writer
  thread, so a plain PUT carries the same meaning). MDL off: PUT
  `/tidb/ddl/all_schema_versions/<ddlID>` = version under a 90-second
  session lease (`util.SessionTTL`); `Init` writes `"0"` there at startup.
- The `ddlID` is the server-info uuid — the same id the owner names in its
  wait set, read here from the node's `serverinfo_syncer::Syncer`.

## Implementation steps

1. `tidb-exec`: `mdl_info_load.rs` — read `mysql.tidb_mdl_info` through
   `SystemTableView` on a read-only transaction (the account/stats loaders'
   exact shape), returning `(job_id, version)` rows.
2. `tidb-server` `cluster_session_node/schema_sync.rs`:
   - `SchemaPinRegistry`: live work registers (connection id, catalog
     version) while a statement or an explicit transaction is running;
     `oldest_pinned()` is the MDL gate. Conservative against Go: Go blocks
     only jobs touching pinned TABLES; this blocks on any older pin, which
     acks later, never earlier.
   - the ack decision as a pure function over (loaded version, mdl rows,
     oldest pin, acked cache) — unit-testable without etcd or TiKV.
   - `SchemaSyncAck` runner thread: on the catalog reload cadence, read the
     rows, decide, PUT acks; maintain the leased non-MDL self key.
3. Wire pins in `cluster_session_node/mod.rs`: statement scope in
   `with_bound_statement`, transaction scope in `open_explicit` →
   `commit_explicit`/`discard_explicit`, guard-dropped on disconnect.
4. Boot: spawn the runner beside the server-info runner (it needs that
   syncer's id), drop it in the same early slot.
5. Receipt: the sysbench ladder's rung 3c passes — Go's `CREATE DATABASE`
   completes with the Rust node registered — and the full ladder runs.

## Progress

- [x] (2026-08-23) Root cause receipted from the Go owner's own log; Go
  contract read (`schemaver/syncer.go`, `issyncer/syncer.go`); node seams
  mapped (watch/reload, etcd client, server-info id, system-table reads,
  session pins).
- [x] (2026-08-23) Steps 1-4 implemented; unit receipts for the decision
  function and the pin registry.
- [x] (2026-08-23) Step 5: ladder rung 3c passes (Go DDL unblocked in 3s);
  full ladder to completion.

## Surprises & Discoveries

- The node was already VISIBLE but mute: server-info registration (landed
  earlier for `TIDB_SERVERS_INFO`) is what put it into the owner's wait set.
  Registering without acking is strictly worse than not registering.
- `EtcdClient` has no Txn support, so Go's `PutKVToEtcdMono` compare-and-put
  cannot be copied literally; single-writer-per-key makes the plain PUT
  equivalent (recorded in the Decision Log).
- `mysql.tidb_mdl_info` is fully readable through the existing
  `SystemTableView` machinery: `job_id` is the clustered int handle, and
  `version` sits in the row value; no new decode surface was needed.

## Decision Log

- Decision: ack keys are written with plain `put`, not a mod-revision
  compare. Rationale: Go's mono guard protects one node's key from that
  node's own concurrent writers; this port has one ack thread, and versions
  only grow. Date/Author: 2026-08-23 / session c4d12b28.
- Decision: the old-schema gate is "no live statement or transaction pins a
  version below the job's", not Go's per-table check. Rationale: strictly
  conservative (acks later, never earlier), needs no table-id plumbing, and
  the table-scoped refinement can land behind the same registry. Date/Author:
  2026-08-23 / session c4d12b28.
- Decision: SUPERSEDED same day -- the gate is now Go's per-table check.
  `RemoveLockDDLJobs` (`session.go:3935-3962`) blocks a job only while live
  work holds one of the JOB'S tables at a version below the job's, recorded
  at statement bind (`preprocess.go:2243-2270`, first use wins, read-only
  autocommit exempt) and dropped with the transaction (`TxnCtx.Cleanup`).
  Ported end to end: `tidb_mdl_info.table_ids` decoded exactly as
  `util.Str2Int64Map` (empty string -> {0}, matching no table); the driver
  session records bound tables through a sink at its one statement funnel;
  the registry keeps `table id -> first-use version` per connection. One
  conservative divergence kept: Go's planner sees through views to base
  tables, our statement names do not, so an unresolvable name marks the
  connection `unresolved` and it blocks under the old whole-transaction
  rule -- later than Go, never earlier. Receipt: the rung-8 DDL that took
  39s under the whole-transaction gate now lands in 0-5s, Go's own pace,
  across two full clean ladders. Date/Author: 2026-08-23 / session
  c4d12b28.
- Decision: the leased `/tidb/ddl/all_schema_versions/<id>` key is kept
  CURRENT on every reload, where Go updates it only when MDL is off.
  Rationale: the key is read only by MDL-off owners; keeping it fresh serves
  them in both modes, and a stale-but-present key is the one shape that can
  block a cluster. Date/Author: 2026-08-23 / session c4d12b28.

## Outcomes & Retrospective

Shipped in `b0580298c4`. The node now acknowledges schema versions to a
Go DDL owner, and the full sysbench ladder against a real TiUP cluster
(PD + TiKV + a Go tidb-server on the same store) runs rung 0 through
rung 7 with **32 OK and zero failures**: the Go control DDL that used to
hang 17 minutes passes, all eight workloads run in both protocols, and
the post-run checksum from the Rust node equals the Go node's byte for
byte (`1000 500500 506087 1 1000`). Nine `schema_sync_acked` receipts,
no ack failures.

Two aggregate-pushdown gaps surfaced only because the acknowledger
unblocked the rungs past it -- the wait had been masking them. Both were
Go-fidelity bugs in typing and in refusal handling, not in the new code:
untyped aggregate leaves (Go types both halves in `AggFuncToPBExpr`) and
a 1105 where Go simply declines to push down. That is the pattern worth
keeping: each blocking bug removed reveals the next one, so a ladder is
worth more than a single pass/fail.

Retrospective: the root cause was a *partial* port. Server-info
registration landed alone for `information_schema.TIDB_SERVERS_INFO`,
and it silently opted this node into a protocol whose other half did not
exist — the Go owner's wait set. Registering while mute is strictly
worse than not registering at all. Before porting the visible half of
any cluster protocol, ask what OTHER nodes will now expect of this one.

Known follow-ups: the ladder's performance columns show the Rust node
still 2-4x behind Go per statement, a separate optimization unit, not a
correctness one. (The earlier follow-up here -- the whole-transaction MDL
gate and its measured 39s DDL stall -- is closed: the gate is per-table
now, see the Decision Log.)

## The last rung-8 divergence: FOUND AND FIXED

Under rung 8 the workload died at `COMMIT` with `9007 Write conflict` --
the narrow commit guard's message, not a KV conflict. The static reading
stalled (every mid-transaction rebuild looked guarded), so the guard was
instrumented and the live failure caught:

```
narrow_commit_guard_fired: shared_version=86 base_version=1910
```

`base_version 1910` is a pin taken on the connection's long-lived
catalog; `shared_version 86` is a FRESHLY REBUILT one, its version
counter restarted. The shared catalog was replaced mid-transaction --
through the one unguarded path: the wide node's `BEGIN` flow rebuilt the
catalog TWICE, once before `session.control_transaction` pinned the
driver transaction's `base_version` (correct) and once after, inside the
dispatch match arm (the bug -- its comment claimed "before the pin",
true only for the WIDE pin). A reload or statistics republish landing in
the microseconds between the narrow pin and the second rebuild swapped
the catalog under the just-opened transaction; its COMMIT then failed
the guard. Eight threads at ~1600 BEGINs per run against reloads every
second put roughly one hit per run in exactly one thread -- the observed
shape.

The fix is deletion: one refresh per BEGIN, before both pins, which is
Go's own shape -- one `domain.GetSnapshotInfoSchema(startTS)` per
activation (`pkg/executor/ddl.go` additionally shows a user transaction
can never contain a DDL: `DDLExec.Next` commits it first). Receipt:
three consecutive full ladders with rung 8 at 6/6, the load leg's first
passes ever.

The earlier analysis in this section survives as design notes: the
narrow guard still conflates "schema moved" (Go: 8028 with the
retryable mark, skipped entirely under MDL) with "peer data write"
(Go: 9007) because one version counter serves both -- but with the
sandwich gone, no unguarded writer of the per-connection catalog
remains, so in the WIDE node the guard is now unreachable rather than
wrong. The identity split still matters for the narrow in-process tier,
where two sessions genuinely share one catalog.
