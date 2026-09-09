# `pkg/dxf/framework/taskexecutor` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The direct package contains exactly 12 tracked artifacts and 4,129 lines.
Every production, test, and Bazel file was read in full in the pinned
worktree before editing. The nested `execute` leaf is a separate package and
has its own receipt. This package has no fixtures, `testdata`, generated
source, platform variants, benchmark/fuzz targets, generator inputs, or
`OWNERS` file.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 89 | `680fa526912596bd42a8719f4e8387bd6d32fe36` | `82a09c6d90c671ba633e0b6859a6ddc4cb8e466bdb6b4a3dd2982f168cfcb899` | executor library/test target; 18-shard metadata |
| `interface.go` | 165 | `b34a936373bda28ced0b496f13b23f56554a2806` | `4c34d06dd4756e359ad6c592c67ace3bb4f3bc3509475dbc25e4bd3abb7144d0` | TaskTable, TaskExecutor, Extension, and base step executor contracts |
| `main_test.go` | 49 | `71a78e3e19cda5d129fc2834b0e3c1fb02a1e092` | `262d64bd6ad45b33e135675be58c1bd683186580e9363d89530dee0454d5fccb` | interval reduction and test harness |
| `manager.go` | 433 | `4362d9f6b00e067ea69296d6b1b5a49020d312ef` | `d22973a8be69e26f745060a2002bccda59cc632514f89953277b29e2c1228ff8` | executor manager lifecycle, retries, runtime ownership |
| `manager_test.go` | 789 | `605486b2d0f462be4748aebf3945bb2f3aac1e59` | `9df01ada11ba8aa6f1d15d35551b305255ec0ee859c61817ed989915bbac9146` | manager and cross-keyspace runtime tests |
| `register.go` | 41 | `f08e3bb449984cb90a63a83e4a3eeb657c9408ae` | `d1c31d1b8d2d14ae6bdd9b8c8b3d06838069be1de2acec9dbd574af7851b8e1b` | task executor factory registry |
| `register_test.go` | 38 | `197efd66196c8a83c8a7a1c5c6b2789abdb0abbc` | `646b847e185b09ec684964eb8e3de68c8b78ce8e9ad5444b29855ca8be16d984` | registry tests |
| `slot.go` | 146 | `d9d6953cfbc850f2f413187a2a041225fc0d2853` | `d748747823c2041c9923a8705f3df46bc41ebd64fb560f28559d802bcc2c2a4e` | slot allocation/exchange |
| `slot_test.go` | 180 | `eac9571db570b50ca07ac9c689b3d4e204db352d` | `c60041c0b619953f1f88d1c6bb8e890c2e1ad59edc1f2a9fd5f4581081396c83` | slot manager tests |
| `task_executor.go` | 807 | `87640c7541ddcda7eef9417e7afaf9ffbb7cedd7` | `aafbe57532fb49688ca94c32f9381a8d2512366168fc914f776f5a40401a904b` | BaseTaskExecutor run/cancel/cleanup lifecycle |
| `task_executor_test.go` | 1,285 | `a32330713c47edd35480f57996440e6fe12f7d98` | `f9962dc28ad2ee00cfe8a522c68dd73bf12819489b084384a50cf74b2047e291` | executor lifecycle, cancellation, and logging regressions |
| `task_executor_testkit_test.go` | 107 | `45bf63b625e80d83e0dd65f8aee85e6e78b12d96` | `d18bebb78610991db2ca97a800fe61f00e5534288dd7e0ea818e25dfce7d7272` | SQL-backed testkit integration |

The package declares 84 production functions/methods, 20 top-level tests,
and 39 test/helper functions. The Go-master delta removes the stale
`GetTasksInStates` requirement from the TaskTable contract, treats
`ErrCancelSubtask` as expected cancellation, adds `BaseTaskExecutor.GetExecID`,
and adds observer assertions that cancellation is not logged as failure.

## Rust ownership and parity decision

Rust has no dependency-closed DXF task-executor manager, slot allocator,
session-backed task table, or StepExecutor runtime. Generic `tidb-dxf` values
cannot replace these lifecycle contracts. No Rust-only executor behavior was
found to remove and no disconnected Rust runtime was invented.

## Validation and risk

Profile: **Ready** for the code/test batch. The complete failpoint-aware suite
passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/taskexecutor -count=1
# PASS; ok github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor 16.116s
```

Failpoints were enabled before and disabled after the suite. `make
bazel_prepare` is required because Go/Bazel files changed; the local gate is
blocked by the unavailable `bazel` executable. Shared `make lint`, Rust fmt,
and `git diff --check` are required before publishing. Main risks are keeping
cancellation classification stable and ensuring generated mocks/interfaces
remain synchronized when the scheduler contract advances.
