# `pkg/dxf/framework/proto` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly 11 tracked artifacts and 1,283 lines. All
production, test, and Bazel files were read in full in the pinned Go-master
worktree before editing. There is no `doc.go`, fixture, `testdata`, generated
source, platform variant, benchmark/fuzz target, or generator input.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 31 | `0d9b06d34ee9cd4af7842a63bb78f2786b7591f4` | `b807f3034c9e4cf39147c00356e1bf62f15fad25e3c0ce0eb652646abef32d8e` | Go library/test target; 11-shard test metadata |
| `modify.go` | 61 | `87d79ad57075b4d82af727212ab3d504fa195739` | `91eecee4adefd31cfd1a26ae908539a91412490b72f22bf4ce616a26cbd10624` | task modification parameters |
| `node.go` | 94 | `92f4723d42b637797940a893abb3ca78c028c755` | `a4ae7416fc5afb5d2e2b508ea17231d0908f193eaab2efc662d7500563c763aa` | managed-node resource records |
| `step.go` | 208 | `cad663754c76fd2cccee6c583df3a12f0bbaf7d8` | `7e3545f34c5a48382473dbab88c83eeceb1ad1dca1924cc610770d5e1c73696c` | framework and business steps |
| `step_test.go` | 70 | `52fce21d39635fbadbf605f0d444da70b23498fa` | `35f3ebdd903ce4a402e0021ef607efed901a82cc500636316b0635ceb0aabf22` | step value and validity tests |
| `subtask.go` | 177 | `0cd2116a1ffe3a03f6edb18a059c31ecf421aa9b` | `2cbd9fa51a56678c5d8354c5af31620f942d1fa5f20038c77fb999505ae43a93` | subtask records and resource allocation |
| `subtask_test.go` | 73 | `987522ec4c13ae062e59c6bbba5bbd2cdffa1a81` | `f8caefe6e4f98aa1e67a71ec4a1bfcc8bb6a4b20c12820d2dd849897df52a102` | subtask terminal/allocation tests |
| `task.go` | 305 | `a98a1cff53ff99dae5badd312d31da3457bc6fc9` | `01b84d9a93f81b28eb2c5b47c3d28223329389be19a6b5fe5b10116d81e73c6c` | task states, limits, runtime slots, cleanup-batch knob |
| `task_test.go` | 172 | `3d816fd0722d7dca7534a19d925ef3829422b07c` | `927129bdfe0d326bec9bed007efb0ac7f0a1ff3903900b16d4195312cd9dd1d1` | 11 top-level task/limit tests, including cleanup bounds |
| `type.go` | 52 | `c8137da4db34d1c829f16bb6ba5d47b36d80fe24` | `ea7d1e562e5e15faf52cd8b1053dccb8fe1c52d375466f519a420f3d983375e6` | task type values and encoding |
| `type_test.go` | 40 | `5354e10e0d54f5b8826fa8800ead67484b6964ac` | `19db58940ce8e8744c7dcc0b819b2f8bb91de8e4292e859f39fead6dbbf52bdf` | task type tests |

The Go-master package declares 55 functions and 11 top-level tests. The
change in this batch adds the owner-local cleanup batch-size constants,
atomic setting/getter, validation, and restore-for-test helper, plus the
focused `TestTaskCleanupBatchSize` regression and one Bazel shard.

## Rust ownership and parity decision

Rust `tidb-dxf` owns the generic task, step, subtask, node, and modification
value vocabulary, but it has no dependency-closed SQL-backed DXF storage or
owner-local HTTP tuning surface. No Rust-only behavior was removed and no
disconnected Rust storage API was invented. The cleanup-batch setting remains
a Go-native integration contract until a Rust scheduler/storage owner exists.

## Validation and risk

Profile: **Ready** for this code-and-test batch. The focused suite passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/dxf/framework/proto -count=1
# ok github.com/pingcap/tidb/pkg/dxf/framework/proto 0.411s
```

`make bazel_prepare` is required because Go sources, tests, and Bazel
metadata changed. Rust formatting, `make lint`, and the storage failpoint
suite are shared Ready gates for the batch and are recorded in the storage
receipt/ExecPlan. The remaining risk is that a future Rust scheduler owner
must preserve these bounds and reset semantics when it becomes dependency
closed.

## Rust-only return-contract alignment (2026-09-06)

The complete 11-artifact Go inventory above remains the authority for this
package. The Rust `tidb-dxf` owner modules (`step`, `task_type`, `task`,
`subtask`, `node`, `modify`, and `schstatus`), inline tests, and workspace
registration were rechecked before this follow-up. Go permits discarding the
results of `Step2Str`, `IsValidStep`, `IsValidBusinessStep`, `Type2Int`, and
`Int2Type`; Rust had imposed five Rust-only `#[must_use]` diagnostics on those
direct source APIs. The annotations were removed without changing step
rendering, validity rules, or task-type integer mappings.

`step::tests::go_step_returns_may_be_ignored_like_go` and
`task_type::tests::go_task_type_returns_may_be_ignored_like_go` discard all
five returns under `#[deny(unused_must_use)]`. Before the source edit, the
focused compile failed with exactly five `unused_must_use` diagnostics; after
the edit both regressions pass. No Go source, Bazel metadata, Cargo
dependency, generated input, fixture, or platform variant changed, so
`make bazel_prepare` was not required.

The full owner suite (13 inline tests), all-target check, Rust formatting,
Ready `make lint`, and `git diff --check` all pass for this package-scoped
follow-up. No Go source, Bazel metadata, Cargo dependency, generated input,
fixture, or platform variant changed, so `make bazel_prepare` was not
required.

```text
OPENSSL_DIR=.../openssl-build/install OPENSSL_STATIC=1 \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml \
  -p tidb-dxf --lib go_ --offline --locked
# PASS; 2 tests

OPENSSL_DIR=.../openssl-build/install OPENSSL_STATIC=1 \
cargo +nightly-2026-08-22 nextest run --manifest-path rust/Cargo.toml \
  -p tidb-dxf --offline --locked --no-fail-fast
# PASS; 13 tests

OPENSSL_DIR=.../openssl-build/install OPENSSL_STATIC=1 \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml \
  -p tidb-dxf --all-targets --offline --locked
# PASS

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# PASS
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint
# PASS
git diff --check
# PASS
```
