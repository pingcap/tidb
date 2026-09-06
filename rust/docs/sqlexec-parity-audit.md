# pkg/util/sqlexec parity audit (baseline a85e0fd5df)

Full-file audit of Go `pkg/util/sqlexec` (restricted_sql_executor.go,
simple_record_set.go) against `rust/crates/tidb-sqlexec/src/lib.rs`.

## Result: no behavior-breaking divergences

- RestrictedSQLExecutor: all 3 methods with matching shapes; the
  ParseWithParams placeholder contract (`%?` auto-conversion, `%%`, `%n`
  identifiers, injection caveat) is restated in the Rust doc this batch.
- ExecOption: all 10 fields; all 9 option funcs present
  (AnalyzeVer2, GetPartitionPruneModeOption, GetAnalyzeSnapshotOption,
  ExecOptionWithSnapshot, ExecOptionWithSysProcTrack with the
  track/untrack pair, UseCurSession, UseSessionPool, IgnoreWarning,
  EnableDDLAnalyze); GetExecOption folds left-to-right from the zero
  value on both sides.
- SQLExecutor/SQLParser/Statement/RecordSet/DetachableRecordSet/
  MultiQueryNoDelayResult shapes match (nil RecordSet as Option,
  `NewChunk(nil)` as `new_chunk(None)`, Send+Sync preserving the
  concurrency note, TryDetach 3-return via DetachResult).
- DrainRecordSet(DrainRecordSetAndClose): chunk ladder matches Go's
  chunk.Renew; close always runs and its error is logged, not
  propagated; ExecSQL short-circuits and closes silently.
- SimpleRecordSet field/next/new_chunk/close semantics match.

## Documented narrowings

- On a mid-drain Next error Go returns partial rows alongside the error;
  Rust discards them (no caller observes the difference today).
- Rust-only seam adaptations: ExecutionContext/TrackProcess/
  SessionVariables traits, DetachResult, SqlExecError/Result aliases,
  SimpleRecordSet::new, result_field_types — intentional seam shapes.

## Validation

- `cargo test -p tidb-sqlexec`, `cargo fmt`, `git diff --check`,
  `make lint`.
