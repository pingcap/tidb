# `pkg/sessionctx/variable` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`febee17ec716d86b1e355e5400ef9e4f4f190bad` (2026-09-02).

This receipt records a bounded, dependency-closed behavior batch within the
large variable package. It does not claim that the complete `SessionVars`,
slow-log, status-variable, sequence-state, or variable-test surface has been
transcreated. Those remaining owners stay explicit below.

## Complete Go package inventory

The package has exactly 31 tracked artifacts and 18,540 lines in the comparison
snapshot: 6 production Go files, 10 production/test support files, 11 Go test
files, 3 Bazel files, and `OWNERS`. Every production file, test, fixture/build
input, generated/platform variant, benchmark, and nested `tests/slowlog`
artifact was read before editing.

| artifact | lines |
| --- | ---: |
| `BUILD.bazel` | 134 |
| `OWNERS` | 11 |
| `embedding_vars.go` | 83 |
| `embedding_vars_test.go` | 141 |
| `error.go` | 52 |
| `main_test.go` | 34 |
| `mock_globalaccessor.go` | 131 |
| `mock_globalaccessor_test.go` | 57 |
| `nextgen_test.go` | 84 |
| `noop.go` | 649 |
| `removed.go` | 68 |
| `removed_test.go` | 29 |
| `sequence_state.go` | 69 |
| `session.go` | 3,995 |
| `setvar_affect.go` | 158 |
| `slow_log.go` | 1,216 |
| `statusvar.go` | 178 |
| `statusvar_test.go` | 66 |
| `sysvar.go` | 4,354 |
| `sysvar_test.go` | 2,413 |
| `tests/BUILD.bazel` | 43 |
| `tests/main_test.go` | 35 |
| `tests/session_test.go` | 1,083 |
| `tests/slowlog/BUILD.bazel` | 25 |
| `tests/slowlog/main_test.go` | 34 |
| `tests/slowlog/slow_log_test.go` | 707 |
| `tests/variable_test.go` | 743 |
| `tidb_vars.go` | 69 |
| `variable.go` | 594 |
| `varsutil.go` | 557 |
| `varsutil_test.go` | 728 |

There are no checked-in fixtures, generated files, platform-specific source
variants, fuzz corpora, or generator inputs beyond the three Bazel manifests.
Temporary certificate material is created by tests and is not a package
artifact.

## Implemented Go behavior

The original Rust catalog had 952 entries and omitted 13 names present on Go
master. The Rust `tidb-session` owner now registers all 13 with their Go
scope, defaults, types, and bounds:

* six embedding API keys and the OpenAI-compatible API base;
* analyze-store batch size, connection-event logging, FULL OUTER JOIN, and
  transaction-file enablement;
* transaction-file minimum mutation size; and
* plan-replayer file retention duration.

The new process-wide `embedding` owner follows Go's ordinary `SET GLOBAL`
path: HTTPS/host allowlist validation and endpoint normalization, default URL
resolution, provider-key masking, raw provider access, and
`EmbeddingConfigVersion` increments only when an effective value changes.
Global reads are redacted while provider code can read the raw key. The Go
transaction-file minimum (zero or at least 1 MiB) is enforced by the shared
system-variable validator. The registry count is now 965 (961 base catalog
entries plus the four workload-repository entries).

The three previously ignored embedding source stubs in `tidb-vardef` remain
there only as leaf-crate documentation: their executable owner is now
`tidb-session`, which can exercise the real registry and SQL global-write
path without introducing a forbidden vardef→session dependency.

The 2026-09-02 Go package batch restores the missing query cop-store limit
contract in the complete `pkg/sessionctx/variable` boundary: a
`SessionVars.QueryCopStoreLimit` field initialized to the Go default, the
global/session `tidb_query_cop_store_limit` registration with Go validation and
setter semantics, and hint-updatable registration. `TestTiDBQueryCopStoreLimit`
pins default initialization and session updates. This is a bounded package
batch; the unrelated embedding, transaction-file, outer-join, and other
variable deltas remain explicit boundaries for later package work.

## Regression and validation evidence

Go-master focused source regressions were run in the detached worktree at the
exact comparison commit with failpoints enabled and disabled by the wrapper:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable -run 'Test(NormalizeOpenAIEmbeddingAPIBase|GetOpenAIEmbeddingBaseURL|EmbeddingAPIKeySysVars)$' -count=1
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable -run 'Test(TxnFileSysVars|TiDBAnalyzeStoreBatchSize|EnableFullOuterJoin|TiDBForeignKeyCheckInSharedLockGate)$' -count=1
```

The new focused command passed in 0.556s before the full run; the pre-fix
compile failed first on the missing `vardef` query-limit constants and then on
the missing `SessionVars` field. The full variable package failpoint-aware run
passed in 0.534s. Rust source-shaped regressions and owner checks passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
./tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable -run '^TestTiDBQueryCopStoreLimit$' -count=1 -vet=off
# passed in 0.556s

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
./tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable -count=1 -vet=off
# passed in 0.534s

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
make lint
# passed

git diff --check
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
TMPDIR=/tmp/tidb-codex \
make bazel_prepare
# blocked: make: bazel: No such file or directory
```

```text
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --lib embedding -- --test-threads=1
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --lib the_registry_is_complete_and_sorted -- --test-threads=1
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --lib 'sysvar::tests::' -- --test-threads=1
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --lib tests_global_vars -- --test-threads=1
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
```

The repository Ready profile also requires the bundled lint gate; it is run
before this batch is committed and pushed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
```

Go production and test sources changed, and a new top-level regression was
added, so `make bazel_prepare` was required and attempted; it is blocked
locally because `bazel` is not installed (`make: bazel: No such file or
directory`). The concurrent worktree also carries unrelated Go module updates;
they were not staged in this package commit.

## Risks and remaining boundaries

The process-wide embedding slots intentionally mirror Go's atomics; callers
must use the raw-key accessor only for provider requests, never SQL output.
Global persistence remains the existing in-memory Rust boundary, and duration
handling for the plan-replayer setting remains declarative until its broader
runtime owner is ported. The package's large `SessionVars` state machine,
slow-log parser/evaluator integration, status-variable map, sequence state,
removed/no-op compatibility layer, mock accessor, and nested integration test
suite still require their own dependency-closed batches. Full Go package,
Bazel shards, and the full Rust workspace were not run for this checkpoint.

## 2026-09-02 Go-master source restoration

Against fetched Go master `78cac443a4f46c13bfe27eb247b5c80657952547`, the
branch was missing the session-side `tidb_analyze_store_batch_size` contract.
`SessionVars` now initializes `AnalyzeStoreBatchSize` to Go's default, and the
global/session registry validates the unsigned range `0..8` before updating the
session field. `TestTiDBAnalyzeStoreBatchSize` pins default initialization and
the session setter path; it passes in the detached Go-master Ready test
worktree. The package's unrelated large registry deltas remain outside this
batch.
