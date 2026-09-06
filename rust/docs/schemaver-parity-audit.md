# pkg/ddl/schemaver parity audit (baseline a85e0fd5df)

Full-file audit of `pkg/ddl/schemaver` (syncer.go, mem_syncer.go) against
`rust/crates/tidb-schemaver`, plus a transport-level check of the etcd
deadline surface in `tidb-pd-client`/`tidb-server`.

## Fixed this batch

1. Session keepalive resilience (`etcd_syncer.rs`): clientv3's `KeepAlive`
   retries transport failures internally and closes `Done` only when the
   lease is truly gone; the port previously closed `done` on ONE failed
   `lease_keep_alive_once`, spuriously forcing syncer restarts on transient
   RPC errors. `done` now closes only after a full `SESSION_TTL_SECONDS`
   elapsed without a single successful round (the lease has then certainly
   expired server-side); context end still closes it promptly. Regression:
   `session_survives_transient_keepalive_failures`.
2. Job-version watch require-leader (`EtcdWatchOps::watch` + Go
   syncer.go:519): the trait gained Go's `WithRequireLeader` flag. The job
   mirror watch passes `true` (Go sets it only there), the global-version
   watch `false`. The production adapter documents that the etcd-client
   crate exposes no gRPC metadata hook to enforce it in transport yet; the
   seam now carries the Go semantics.

## Resolved by transport (no code change needed)

- Per-op 2s timeout (`etcd.KeyOpDefaultTimeout`): every etcd command the
  syncer issues is bounded in the `tidb-pd-client` KV worker by
  `across_endpoints(..., timeout, ...)` -- with per-command overrides for
  put/delete/get -- so no hung RPC can block the retry loops forever. The
  remaining nuance is WHICH constant: the client timeout is chosen at
  connect time; the server wiring (not yet built) should pass
  `KEY_OP_DEFAULT_TIMEOUT` (2s) to match Go's per-op constant exactly.

## Verified matching (one line each)

- Constants and key layouts: InitialVersion/0, retry counts (1-shot 3,
  unlimited), checkVersInterval 20ms, ddlPrompt, CheckVersFirstWaitTime 50ms
  settable, session TTL 90, `/tidb/ddl/...` paths byte-identical.
- Init: CAS seed of the global key, 3-retry session, watch, leased self-put.
- Restart/Done: unlimited session retries, 2s-bounded unlimited leased put,
  session swap; Done = lease loss or ctx end.
- WatchGlobalSchemaVer/GlobalVersionCh/Rewatch: channel swap semantics.
- UpdateSelfVersion: MDL-on jobID==0 early return, mono CAS x3, MDL-off
  leased unlimited; OwnerUpdateGlobalVersion: unlimited, ctx per attempt.
- WaitVersionSynced: 50ms first wait (MDL off), intervalCnt 50, etcd-poll
  with updatedMap cache, per-round server rebuild, newest-instance-wins
  calculateUpdatedMap incl. assumed count, matchFn empty-map guard, 1s
  timeout rounds with clearMatchFn.
- SyncJobSchemaVerLoop + helpers: 1-in-50 log cadence, snapshot-as-PUT
  replay, watch from header.Revision+1, map cleanup/pruning.
- nodeVersions: add/del/len/matchOrSet/clearData/clearMatchFn/
  emptyAndNotUsed incl. onceMatchFn run-and-clear under lock.
- SyncSummary + String(): both format strings byte-identical.
- mem_syncer: all 11 interface methods, buffered-1 channels, non-blocking
  send, nil-channel semantics, 2ms WaitVersionSynced tick.
- MDL gate: next-gen-always-true rule; assumed keyspace; GenerateExecID.

## Accepted narrowings (documented)

- Go panics on nil-session Done/lease paths; Rust returns a never-channel /
  unleased put (unrepresentable states).
- `putKeyNoRetry = 1` is dead in Go and unported.
- metrics (DeploySyncerHistogram etc.) and failpoints are replaced by
  in-process seams; no control flow reads them.
- NewSession abort condition is narrower than Go's dead-client detection.
- Rewatch stops the old watch stream synchronously (Go leaves the old
  channel live for existing holders).
- `job_schema_ver_match_or_set` evaluates a fresh match fn immediately
  instead of installing it unrun; safe while match fns keep Go's
  empty-map guard.

## Validation

- `cargo test -p tidb-schemaver --all-targets`: 9 unit + integration tests
  pass (incl. the new keepalive regression).
- `cargo build -p tidb-server` (adapter change) clean; `cargo fmt`;
  `git diff --check`.
