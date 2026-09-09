# `pkg/domain` Go-master plan-replayer retention parity receipt

Comparison source: Go `origin/master` at commit
`1c1a334d2be1dce64888b6e1f054462c566b0734` (2026-09-02).

This receipt records the complete direct root-package inventory before the
bounded edit. It does not claim that the whole `pkg/domain` package has been
transcreated: the domain lifecycle, metadata/etcd synchronization, SQL
executor, plan-replayer dump writer, and nested domain packages remain
dependency-closed boundaries for later package units.

## Complete Go root-package inventory

The direct root package contains 31 tracked artifacts (29 Go files, one
`BUILD.bazel`, and `OWNERS`) totaling 9,140 lines at the comparison commit.
The 29 Go files comprise 16 production/support files and 13 tests; the test
set includes skipped/integration cases, benchmarks, and failpoint-backed
helpers where present. Every artifact below was read in full before editing.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 225 | `domain` library and 30-shard `domain_test` target, source lists and dependencies |
| `OWNERS` | 10 | `sig-approvers-domain` ownership |
| `db_test.go` | 203 | database/domain integration helpers and tests |
| `domain.go` | 3,132 | Domain lifecycle, workers, synchronization, and GC loop |
| `domain_sysvars.go` | 165 | domain-owned system-variable synchronization |
| `domain_test.go` | 662 | lifecycle, sysvar, and worker tests |
| `domain_utils_test.go` | 32 | domain utility test helpers |
| `domainctx.go` | 29 | domain context implementation |
| `domainctx_test.go` | 40 | domain context tests |
| `extract.go` | 534 | plan/extract task coordination |
| `extract_test.go` | 140 | extract task tests |
| `historical_stats.go` | 100 | historical statistics worker |
| `inference.go` | 33 | inference worker boundary |
| `main_test.go` | 38 | package test setup |
| `optimize_trace.go` | 38 | optimizer trace worker |
| `plan_replayer.go` | 596 | plan-replayer task state and GC checker |
| `plan_replayer_dump.go` | 1,004 | plan-replayer dump serialization and storage |
| `plan_replayer_handle_test.go` | 234 | plan-replayer handle tests |
| `plan_replayer_slow_log_test.go` | 114 | slow-log capture tests |
| `plan_replayer_test.go` | 174 | plan-replayer task/GC tests |
| `resource_group_controller_options.go` | 66 | resource-group controller options |
| `ru_stats.go` | 298 | RU statistics worker |
| `ru_stats_test.go` | 173 | RU statistics tests |
| `runaway.go` | 57 | runaway query worker |
| `runaway_test.go` | 290 | runaway query tests |
| `schema_checker.go` | 81 | schema checker |
| `schema_checker_test.go` | 72 | schema checker tests |
| `sysvar_cache.go` | 174 | cached system-variable state |
| `test_helper.go` | 58 | package test support |
| `topn_slow_query.go` | 224 | Top-N slow-query worker |
| `topn_slow_query_test.go` | 144 | Top-N slow-query tests |

There is no `doc.go` in the direct root. There are no direct fixture,
`testdata`, generated-source, generated-input, platform-variant, fuzz, or
benchmark-only artifacts outside the files listed above. The nested packages
are separate Go package boundaries and were inventoried rather than folded
into this root claim: `affinity` (4 artifacts), `crossks` (8),
`globalconfigsync` (3), `infosync` (12), `metrics` (2), `serverinfo` (5), and
`sqlsvrapi` (6, including its generated `mock` subpackage).

## Rust owner inventory and decision

The corresponding Rust owner is `rust/crates/tidb-domain`, with
`Cargo.toml` plus 17 source modules: `cdcutil.rs`, `disttask.rs`,
`domain_sysvars.rs`, `domainutil.rs`, `historical_stats.rs`, `lib.rs`,
`optimize_trace.rs`, `plan_replayer.rs`, `replayer.rs`, `ru_stats.rs`,
`schema_checker.rs`, `serverinfo.rs`, `serverinfo_syncer.rs`,
`status_endpoint_claim.rs`, `sysvar_cache.rs`, and `topn_slow_query.rs`.
The owner contains 143 in-module Rust tests. `plan_replayer.rs` already
exposes `DumpFileGcChecker::gc_dump_files`, taking separate default and
capture retention durations, so no Rust production edit or Rust-only facade
was needed for this caller-level behavior.

The Go-master delta from commit `6cbbd222c7` changes
`DumpFileGcCheckerLoop` to read the process-global
`vardef.GetPlanReplayerFileRetentionTime()` on each ticker round instead of
using a hard-coded one-hour default. The Go branch already contains the
vardef runtime API restored by the preceding `pkg/sessionctx/vardef` batch.
This batch restores that missing domain integration and keeps the lookup at a
small, directly testable domain-boundary helper. The helper reads the current
global on every call, so changing the setting affects the next GC round
without changing capture-file retention (still seven days).

## Regression and validation evidence

The focused regression was run before and after the edit:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/domain -run '^TestPlanReplayerGCDefaultDurationUsesVardef$' -count=1
# before: compile failure, undefined: planReplayerGCDefaultDuration
# after:  ok github.com/pingcap/tidb/pkg/domain 1.427s
```

The canonical failpoint-aware focused and full root-package suites both pass:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/domain \
  -run '^TestPlanReplayerGCDefaultDurationUsesVardef$' -count=1
# ok github.com/pingcap/tidb/pkg/domain 1.567s

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/domain -count=1
# PASS ok github.com/pingcap/tidb/pkg/domain 21.629s
```

Ready-profile repository gates for this batch are:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
```

`make bazel_prepare` is required because Go production and test sources were
changed and a new top-level Go test was added. The local run is blocked by the
unavailable `bazel` executable (`make: bazel: No such file or directory`). No
real etcd, external-storage, or plan-replayer dump integration was exercised;
those are outside this caller-level regression.

## Risks and outcome

The behavior risk is process-global mutable state: the helper must preserve
the signed `time.Duration` value and observe it at the beginning of each GC
round. The test restores the prior value with `t.Cleanup`, preventing leakage
to other tests. The Rust GC primitive already accepts caller-provided
durations, so this batch introduces no Rust compatibility or performance
risk. The package-level lifecycle and nested domain integration remain
explicitly unverified boundaries for subsequent audits.

## 2026-09-06 plan-replayer dependency lock closure

The current integration tip added `toml` and `tidb-util` as production
dependencies and `tidb-parser` as a development dependency of
`tidb-domain`, with direct uses in `plan_replayer.rs`, but did not add them to
the workspace lockfile's existing `tidb-domain` package entry. Consequently
every `cargo --locked` command failed during workspace resolution before it
could select a package. The pre-fix failure is captured in
`/tmp/tidb-dxf-proto-restored-prefix.log`.

Regenerating the lock metadata offline adds exactly the three dependency names
to that existing entry. It changes no resolved version, checksum, or transitive
package and does not alter runtime behavior. This is a Rust build-artifact
correction for the root `pkg/domain` owner; per the requested Rust-only scope,
no Go source or test was read, edited, or executed.

Ready evidence:

- `cargo +nightly-2026-08-22 metadata --manifest-path rust/Cargo.toml --offline --locked --no-deps --format-version=1`;
- `cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml --offline --locked -p tidb-domain --lib --test-threads=1` (158 passed);
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-domain --all-targets --quiet`;
- `make lint`;
- `git diff --check`.

The focused regression is the frozen workspace-resolution gate itself: it
fails before the lock entry is completed and passes afterward. Adding a Rust
unit test cannot validate Cargo's pre-compilation lock consistency. No Go,
Bazel, module, or import graph changed, so `make bazel_prepare` was not
required. Existing warning debt in the concurrently added plan-replayer source
is outside this lock-only correction and remains visible in the check output.
