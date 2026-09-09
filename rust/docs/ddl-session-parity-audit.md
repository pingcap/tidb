# pkg/ddl/session parity audit (baseline a85e0fd5df)

Full audit of Go `pkg/ddl/session` (session.go, session_pool.go) against
`rust/crates/tidb-ddl-session`.

## Result: no behavior-breaking divergences

- Session ops (Begin/BeginPessimistic/Commit/Txn/Rollback/Reset): order
  and arguments identical, including `StmtRollback(bg,false)` before
  `RollbackTxn`.
- Execute: prometheus
  `tidb_ddl_job_table_duration_seconds{type,...}` with the same
  exponential buckets and ok/err suffixes; timer covers panic paths;
  request-source defaults to `ddl`; DrainRecordSet(...,8); nil-result.
- RunInTxn incl. the NotifyBeginTxnCh failpoint (v==1 send / v==2 receive
  gated on MockDDLOnce) — Condvar stands in for the unbuffered channel.
- Pool Get/Put/Destroy/Close: "session pool is closed" text, the
  autocommit/restricted-SQL/stmt-tz/DiskFullOpt/StoreInternalSession
  ordering, Put's assert+RollbackTxn+ClearDiskFullOpt+Put+DeleteInternal-
  Session sequence, the 3-way Destroy type switch (close+Put(None) for
  ResourcePool, warn fallback), idempotent Close with the same log text.

## Documented narrowings

- Go's Get type-assert error is structurally impossible behind the typed
  Rust pool (noted at the site).
- RecordSetCloser logs close errors where Go's `terror.Call` silently
  discards them.
- The schedule-eval trait surface is forward-port scaffolding with no
  baseline-Go counterpart in this package (noted at the site).

## Validation

- `cargo test -p tidb-ddl-session`, `cargo fmt`, `git diff --check`,
  `make lint`.
