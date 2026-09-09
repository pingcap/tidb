# `pkg/dxf/framework/integrationtests` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly 11 tracked artifacts and 2,210 lines. Every
integration test, benchmark, and Bazel file was read in full in the pinned
Go-master worktree before editing. There are no fixture directories,
`testdata`, generated sources/inputs, platform variants, fuzz targets, or
`OWNERS` files. The package has 26 top-level test functions (including
`TestMain`), one benchmark, and 28 helper functions.

| artifact | lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 51 | `8aec1d2f69286c85a31ccd232e953e330338f737` | `6e80c6c0c74fe18194413eef6cc0ec59a0c758da3587536e420315adf7b3d08a` | 25-shard flaky integration target |
| `bench_test.go` | 177 | `85b6d69292534f480467407b3b1ab177c9827716` | `a9409eb05a6af5ae5339f16d242a35727005c2117dd14548ba946f63cd096e9b` | scheduler startup-overhead benchmark and TiKV setup |
| `framework_err_handling_test.go` | 99 | `85959ee7d5d6fc1ba9067dc08bbaa7c003e2fd9a` | `9938d8eaa74ec9bd3debb2f766b9c068db501ba7e0dd10db814b0f4fbf75dd11` | retry/manual-recovery/error-state integration tests |
| `framework_ha_test.go` | 108 | `d549298b710414e40063252ba7c685ec80fc3881` | `42a5fc68baf017a549806af953c28b38cbf7013cc709e9e1b95fa28016d5972a` | owner failover and random node shutdown |
| `framework_pause_and_resume_test.go` | 101 | `daf05eb857757e785d734b1069b1be657234abde` | `3398e83fd090b02b2657f5ff2e1282c91f8f95184de56857a26c43fc9021c4d4` | pause/resume state and cleanup checks |
| `framework_rollback_test.go` | 45 | `e2a9d85a95933939dee3f66787325c35c86ae244` | `6ae073d52e9d4415752034bcee41814b0a0199230df3844f35c094d7952a3799` | rollback-on-cancel integration test |
| `framework_scope_test.go` | 216 | `09d81932ad8c222b5dff789d5dddd829cb7b5c1b` | `8379c3212feb4bd079a36407542a3980900f159180d5d729de03ff079815b500` | target-scope and node-role behavior |
| `framework_test.go` | 493 | `2bb6d11fb84dd19a79e27672f4fff2aa6c80432c` | `f0c74d9e55d510ac7ffd50d668fe5710b0904e683bf58b2b8b4c7eb068b4a081` | lifecycle, cancellation, GC, cleanup, and slots |
| `main_test.go` | 36 | `cbf96ce4966dce89ebf18c0c25e79d50af277914` | `42e20a8b8d10183a6160fa83038df3a53baf277e6baa086cff7f0febb8225809` | common setup and goleak TestMain |
| `modify_test.go` | 553 | `c413fd237682f87fdd08b26febe114eeb16e87f5` | `e6fccb7f45d1239d000db921f5b9ea704ae9c26afb884c50845a8bbbb05a773e` | task metadata/concurrency/max-node modifications |
| `resource_control_test.go` | 331 | `e44a5119e6583879e8bc8b1aa06175414607bc10` | `1f56a11adceb33d61eb72472ef76365b0919ebf242af75b389c9b7806338eae9` | slot utilization, preemption, and scale tests |

## Go-master alignment

The focused delta updates every cleanup registration to the `Cleaner` API and
renames the cleanup integration test. SQL inspection of `task_key` now follows
the canonical `TaskIDToKey` VARCHAR representation; manual-recovery state
updates no longer quote the numeric task ID. These changes prevent integration
tests from silently exercising the obsolete task-key encoding after the
storage/scheduler cleanup contract update.

Rust has no dependency-closed DXF scheduler, SQL-backed task manager, TiKV
playground harness, failpoint lifecycle, or GoMock integration context. No
Rust-only integration behavior was found to remove and no disconnected Rust
test harness was invented.

## Validation and risk

Profile: **Ready** for this integration-test alignment. The package contains
failpoints and long-running multi-node harnesses; use the failpoint-aware
wrapper for focused runs. The scheduler and testutil parent suites passed
after their contract changes. A complete integration-test run is environment-
dependent and was not run locally; the targeted compile/test command is:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/integrationtests \
  -run '^(TestOnTaskError|TestFrameworkCleaner|TestModifyTaskMaxNodeCountForSubtaskBalance)$' -count=1
```

Because Go test files changed, `make bazel_prepare` is required; the local
gate is blocked by the unavailable `bazel` executable. Ready shared gates are
`make lint`, Rust formatting, and `git diff --check`.

## Outcome

The package remains Go-native integration coverage. Its complete inventory,
canonical task-key/cleaner updates, and validation boundary are recorded here;
repository-wide parity is not claimed and the rolling audit continues.
