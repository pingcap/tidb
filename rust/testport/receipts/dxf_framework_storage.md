# `pkg/dxf/framework/storage` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The pinned package contains exactly 11 tracked artifacts and 4,841 lines.
Every production, test, and Bazel file was read in full before editing. There
is no package `doc.go`, fixture, `testdata`, generated source, platform
variant, benchmark/fuzz target, generator input, or `OWNERS` file.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 74 | `c0d953299000be1912b1aaf54adac4bb364b2f0f` | `bdcbfa3cd743bd84a061506c0f7c361ccef84b97e8a703b813edf17e117b4cb4` | storage library/test target; 34-shard flaky test metadata |
| `converter.go` | 155 | `11172e6df5f95632304af54eed5ca5a37be4c89c` | `c14e607f84a233a3b79f7a505e9ac9a1abe2c54a9f2243c2c1fed1e8bd59701c` | SQL-row conversion and canonical task-key encoding |
| `history.go` | 317 | `8afe4f223b20eabbc5f89a29984acb5de75f5a41` | `99c131eb5e3ac03c37fcb1337b81b4be979610a24be66dc24486d673b56a055b` | atomic history transfer, pagination, redacted error categories |
| `history_test.go` | 35 | `2d68300ce54b8d6837824b52b609b934a3f17faf` | `89619fbb2897dac8d8d946e86a97179b9213d574d2994d19b2106cb2eb81724c` | error-category regression |
| `nodes.go` | 267 | `8e39de794c8d670270d3e9c3900cf9e8a5cdd2ec` | `ba470d8ce7d23f9df06683647f5714f7a6a8a7ed84041368ea90a92055b935fe` | node metadata and resource queries |
| `subtask_state.go` | 165 | `094b2fa339c0370f998bf90438d791aa12855d54` | `7bef5a1adf87b9e9c5f380e5703a2d36648b6064f9faa3fc9c395abba61d93d3` | subtask state transitions and serialized errors |
| `table_test.go` | 1,770 | `b503a268ed3ea7efb55e6f3bcdab03da6f883cfa` | `df24bddd8d49510ff1838381638f7bd39d0bd0ff75ec731fbf58aba321b8bcd1` | SQL table/history, batch-limit, and key-encoding tests |
| `task_state.go` | 339 | `608e0c90873d6d461a6996c458c653a8bfe20f77` | `22b78b3dbbde0ec2260bf1fac51fe3ec2bbaaa9215fa47b6a147e2aa85f4668f` | task state transitions, cancellation, modification |
| `task_state_test.go` | 403 | `9eb3d968edab13e73fe3312eea256038d0e554ec` | `21cecc32cd31f383b58fba67e95ff1bd33bd315e4ca175892cfe411d60545469` | state/error/transaction regressions |
| `task_table.go` | 1,239 | `01b0e6e0dc207cc1d950dd9246a2bb6b719ceac1` | `2d930bfbf1e74346a1f4e9a948e901a56127fe1eee8535d54d08edf7adbf016e` | SQL-backed task/subtask manager and cleanup query |
| `task_table_test.go` | 77 | `ba66953213d49ac8295f651a3146e4333a92cfcb` | `0cf8955253c391783a5fb582fd3d8083676113605f57822272f3d354d1a415f0` | serialization/splitting tests and test harness |

The package declares 105 production functions/methods, 35 top-level tests,
and 39 test/helper functions. The Go-master delta adds canonical decimal
`TaskIDToKey` use for VARCHAR task keys, atomic batch transfer of task and
subtask history, bounded `GetCleanupTasks`, `TaskCleanupInfo`, and safe
history error code/category projection. The existing `GetTasksInStates` query
is retained as a compatibility method for the branch's older scheduler and
task-executor interfaces; new cleanup callers use `GetCleanupTasks`.

## Rust ownership and parity decision

Rust `tidb-dxf` owns task/step vocabulary but no dependency-closed SQL-backed
DXF task manager, transaction/session pool, or history HTTP API. No Rust-only
storage behavior or ignored Rust test was found to remove, and no disconnected
Rust storage implementation was invented. The Go storage boundary remains
explicit until a native scheduler/storage owner can preserve transaction and
schema semantics.

## Validation and risk

Profile: **Ready** for this Go behavior and regression batch. Focused commands
passed:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/storage -count=1
# PASS; ok github.com/pingcap/tidb/pkg/dxf/framework/storage 36.479s
```

The wrapper enabled failpoints before the suite and disabled them afterward.
The first post-port attempt failed to compile because proto lacked
`GetTaskCleanupBatchSize`; after the proto dependency was aligned, the full
storage suite passed. `make bazel_prepare` is required for this batch because
Go source/test files and Bazel targets changed. Rust formatting and `make
lint` remain required shared Ready gates. Main risks are SQL transaction
compatibility, task-key string coercion, and preserving idempotent cleanup
when history transfer is retried.
