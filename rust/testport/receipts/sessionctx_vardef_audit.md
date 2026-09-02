# `pkg/sessionctx/vardef` Go-master parity audit

Comparison source: Go `origin/master` at commit
`1c1a334d2be1dce64888b6e1f054462c566b0734` (2026-09-02).

This receipt records the complete package inventory and two bounded parity
batches implemented in the Go package and Rust `tidb-vardef` owner. It does
not claim the full `pkg/sessionctx/vardef` package is transcreated: the other
mutable runtime globals, the Go `SysVar` registry, `SessionVars`, and
slow-log/session integration still belong to later dependency-closed package
units.

## Complete Go package inventory

The package has exactly seven artifacts (3 production Go files, 2 Go tests, a
Bazel target, and OWNERS), with 3,060 lines in the Go-master snapshot:

| artifact | lines | inventory |
| --- | ---: | --- |
| `BUILD.bazel` | 42 | `go_library` over `runtime.go`, `sysvar.go`, `tidb_vars.go`; `go_test` over the two test files; 2 shards; listed config, kerneltype, joinversion, parser/mysql, slowlogrule, memory, paging, size, tipb, x/time/rate, and atomic dependencies |
| `OWNERS` | 11 | Bazel files use community approvers; package source/OWNERS use critical TiDB-server approvers |
| `runtime.go` | 89 | 9 functions: four lease setter/getter pairs and `IsReadOnlyVarInNextGen`; four process-wide atomic durations |
| `runtime_test.go` | 36 | `TestIsReadOnlyVarInNextGen`; next-gen-only case matrix |
| `sysvar.go` | 352 | `SetNamesVariables`, `SetCharsetVariables`, and the complete system-variable name/mode/scope/type constant layer; no functions |
| `tidb_vars.go` | 2,445 | 22 functions (duration/memory helpers, exchange-compression and scope helpers, clustered-index conversion, DDL/runtime atomic accessors, MDL/assertion policy), name/default/bound constants, and process-global atomics |
| `tidb_vars_test.go` | 85 | `TestIsMDLEnabledInNextGen`, `runConcurrentTest`, and three rate-limiter benchmarks |

There are no package fixtures, generated files, platform-specific variants, or
additional nested build targets. The Go source has 2 production test cases and
3 benchmarks; no failpoint is used by this package.

The Rust owner inventory is `Cargo.toml`, `lib.rs`, `tidb_vars.rs`,
`defaults.rs`, `bounds.rs`, `modes.rs`, `global_sysvar_initial.rs`, the three
in-crate test modules, and the external `tests/tidb_vars_source.rs` carrier.
The owner now includes the plan-replayer retention atomic and its source
regression; the remaining Go runtime/session registry is explicitly
unclaimed.

## Go-master delta and parity decisions

The Rust constants extraction was based on the Go snapshot at
`6bc5d4ccbac9f0f36ebd05af4db98768a00f2467`. Comparing that complete source
with current master found exactly:

* 13 current system-variable names: Analyze store batching, plan-replayer file
  retention, FULL OUTER JOIN, file-based transaction controls, and the seven
  experimental embedding provider variables, plus connection-event logging
  (which was already present in the Go snapshot used for the initial extraction
  but missing from the Rust name table).
* 7 new `Def*` defaults: Analyze store batch size, OpenAI embedding base URL,
  plan-replayer retention (represented as Go `time.Duration` nanoseconds),
  FULL OUTER JOIN, connection-event logging, and the two transaction-file
  defaults.
* 2 new bounds: `MaxTiDBAnalyzeStoreBatchSize = 8` and
  `MinTiDBTxnFileMinMutationSize = 1 << 20`.
* `DefTiDBMergePartitionStatsConcurrency` was removed from Go master. The Rust
  default was deleted as Rust-only behavior; the backward-compatible name
  constant remains because Go still exposes `TiDBMergePartitionStatsConcurrency`
  and registers it with a literal value.

The Rust owner now exports all of those current names/defaults/bounds, updates
the source inventory list, and keeps `tidb-vardef`'s documented totals at 521
name/value constants and 395 `Def*` defaults. Focused regressions assert every
new name and value, including the duration conversion and transaction-file
bound.

The 2026-09-02 Go package batch restores the missing
`TiDBQueryCopStoreLimit` name and `DefTiDBQueryCopStoreLimit` default required
by the Go session-variable registry. `TestQueryCopStoreLimitConstants` pins
the exact spelling and default; the dependent variable sysvar registration is
tracked as a separate `pkg/sessionctx/variable` package batch.

The follow-up runtime batch restores the Go-master
`SetPlanReplayerFileRetentionTime`/`GetPlanReplayerFileRetentionTime` process
global, its `TiDBPlanReplayerFileRetentionTime` name and seven-day default,
and a focused setter/getter regression. Rust stores the same signed nanosecond
representation in an `AtomicI64`, preserving Go's `time.Duration` contract;
the domain GC caller and `pkg/sessionctx/variable` registration remain
separate package boundaries.

## Validation (Ready profile)

Go-master source tests, run in a detached worktree at the exact comparison
commit:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/sessionctx/vardef -count=1
ok github.com/pingcap/tidb/pkg/sessionctx/vardef 0.424s

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/sessionctx/variable -run '^TestTiDBAnalyzeStoreBatchSize$' -count=1
ok github.com/pingcap/tidb/pkg/sessionctx/variable 0.533s
```

Rust owner and source carriers:

```text
cargo +nightly-2026-08-22 test --offline --locked -p tidb-vardef --lib -- --test-threads=1
43 passed, 0 failed, 107 ignored
cargo +nightly-2026-08-22 test --offline --locked -p tidb-vardef --test tidb_vars_source -- --test-threads=1
3 passed, 0 failed
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
```

The repository Ready lint was run with the bundled Go runtime:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
```

The new Go regression failed before the runtime API was restored with
undefined `Get/SetPlanReplayerFileRetentionTime`; it passes after the fix.
The Rust source regression likewise failed before the owner API existed with
unresolved imports, then passes with the implementation.

`make bazel_prepare` is required because this follow-up restores Go source and
test behavior; the local run is blocked by the unavailable `bazel` executable.

## Risks and unverified surfaces

The constants are compile-time API additions/removal only; the runtime batch's
primary risk is an incorrect source spelling, signed nanosecond conversion, or
global-state lifecycle. The Go `SessionVars`/`SysVar` registration and
validation behavior was executed in the Go-master worktree but is not
implemented in the Rust constants crate. The remaining runtime atomics,
embedding hooks, transaction-file wiring, Windows and other unsupported
targets, and the intentionally ignored Rust session/registry tests remain
unverified by this package checkpoint.

## 2026-09-02 Go-master source restoration

Against fetched Go master `78cac443a4f46c13bfe27eb247b5c80657952547`, the
working branch was missing the `tidb_analyze_store_batch_size` name, default,
and upper bound. The constants were restored in `tidb_vars.go` exactly as Go
defines them (`4` default and `8` maximum); the dependent session registry is
recorded in the separate `pkg/sessionctx/variable` receipt. The focused
variable regression failed before the constants existed and passes after the
restoration in the detached Go-master test worktree.
