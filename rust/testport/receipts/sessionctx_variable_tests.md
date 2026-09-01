# `pkg/sessionctx/variable/tests` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

This is a separate package claim from the parent `pkg/sessionctx/variable`
package and from `pkg/sessionctx/variable/tests/slowlog`. The package is a
cross-cutting integration suite; this receipt records its complete inventory,
the exact Go validation boundary, and the dependency-closed Rust owners found
during the audit. It does not claim that the test suite has been transcreated
as one Rust package.

## Complete inventory

The package contains exactly four tracked artifacts and 1,904 lines in the
comparison snapshot. Every production-adjacent test harness, test, build
manifest, import, fixture reference, generated/platform variant, and helper
was read before editing. There is no `doc.go`, `testdata` or fixture directory,
generated output, platform-specific source, fuzz corpus, benchmark, or
generator input in this package.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 43 | `820f01709636ba61e6524d3b7b2f816fa39e3bf9` | `a43225f0482bc40db27c2167ba2e4373fdf3c5fe9e80b5d94208ab86267418c3` | flaky short `go_test`, 47 shards |
| `main_test.go` | 35 | `eaa52297b5d03dad004edfe4fa3754110bba36e3` | `be30a0813c6ca6de604ff1cc2b088513d382e7ce732162f4bce7cab7be95ed87` | TestMain, common setup, and goleak harness |
| `session_test.go` | 1,083 | `25e5edf470bf49a5e2a71768b9309e0375985904` | `c96452c36168cb3025141d3555bc608fd37a16122d28c6df25e4086f58678c4b` | session, slow-log, hook, user-variable, chunk, row-ID, planner, and cloud-storage tests |
| `variable_test.go` | 743 | `74b2f17fc5b4d7a402473aa5b55d33fa9bee989a` | `4215bb47f9d67035ec68281b7b8fb3182d70b571bf796ca205c18b341b170455` | registry, validation, scope, dependency, and setter tests |

The function inventory is 48 `Test*`/`TestMain` declarations plus one helper:
`main_test.go` has `TestMain`; `session_test.go` has 18 tests and
`compareSlowLogItems`; and `variable_test.go` has 29 tests. The BUILD target
declares all three Go test sources and its 47-shard flaky composition.

## Go behavior and validation boundary

The exact Go-master suite was run with the repository failpoint wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable/tests -count=1
```

The run reached the tests but failed in two pre-existing source-level ways:

* `TestHookContext` leaves a global callback whose `require.Equal` assertion
  is invoked asynchronously by later bootstrap work, causing a panic in
  `TestTiDBOptPartialOrderedIndexForTopNSessionAndGlobal`.
* `TestDefaultValuesAreSettable` rejects the current Go default for
  `tidb_auto_analyze_concurrency` while `tidb_enable_auto_analyze=false` (the
  priority-queue prerequisite is true). A rerun excluding `TestHookContext`
  reaches and reproduces this same assertion.

The wrapper disabled failpoints during teardown (refcount returned to zero).
No test or fixture edit was made to hide either failure, and no Go production
behavior was changed in this batch.

## Rust ownership and cleanup

The suite's executable Rust owners are split across dependency-closed crates:

* slow-log formatting and RUv2 rendering are tested by
  `tidb-exec::slow_log_format`; parsing and generic rule matching have their
  own `slow_log_parse` and `slow_log_match` owners;
* sysvar native-value conversion, cache-skipping metadata, and dependency
  ordering are tested by `tidb-session::sysvar`; scope-string rendering is
  tested by `tidb-exec::sysvar_scope_source`;
* registry metadata and selected global/session setters are covered by the
  existing `tidb-session` sysvar and variables suites.

The historical `tidb-vardef` seed contained six empty ignored functions for
those now-owned behaviors. They were removed, and the historical receipts
`b011.md` and `b012.md` now point to the executable owner tests. The remaining
ignored carriers stay explicit because the complete Go tests require
SessionVars/TestKit/Domain integration, savepoints and transaction context,
user-variable synchronization, chunk allocation, row-ID generation,
performance-schema/session hooks, cloud-storage URI state, full slow-log
accessor matching, instance config, and the Go test harness. No dependency-
closed Rust owner for that combined surface exists yet, so no Rust-only
behavior was removed and no speculative replacement was added.

## Validation and risk

Profile: **Ready** for this Rust test-carrier cleanup and receipt update.
The focused vardef suite passed after the stale stubs were removed:

```text
cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-vardef --lib
# 43 passed; 107 ignored

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
git diff --check
```

The focused Rust test command for the new owner could not be run to crate
execution because `openssl-sys v0.9.117` cannot find pkg-config/OpenSSL on
`aarch64-apple-darwin`; this is an environment dependency failure, not a test
assertion. No Go/Bazel/module source changed, so `make bazel_prepare` was not
required. Full Bazel shards, the complete Rust workspace, and a dependency-
closed Rust implementation of this integration package remain unverified.

The cleanup only deletes redundant ignored test seeds and updates audit
receipts; runtime correctness, compatibility, and performance risk are
unchanged. The package remains an explicit parity boundary rather than a
completed Rust transcreation claim.
