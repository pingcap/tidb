# `pkg/dxf/framework/scheduler` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The direct package contains exactly 17 tracked artifacts and 6,321 lines. Every
direct production, test, and build file was read in full in a detached worktree
at the pinned Go commit before this receipt was written. The nested
`pkg/dxf/framework/scheduler/mock` package is a separate generated-support unit
and is not included in these counts. There are no direct fixtures, `testdata`,
generated source/input, platform-specific variants, benchmarks, fuzz targets,
or `OWNERS` files.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 112 | `843b3ab87ae7e7bf37bf29610effb04b8a67e4c1` | `de2b76f6d3a0b8fc3af063a8b22f21d428e470eee1e0161549aabd4ba10924df` | scheduler library and 11-shard flaky test target |
| `autoscaler.go` | 187 | `6c8ca3c6ad8a61fa441d1e91cc25dd9536607da1` | `d5d8ecffcb5654fd9bb9f849af264e1a7eae9203edad46a11e15232db22b4275` | data/store-size resource estimation, node limits, required slots, and DistSQL concurrency |
| `autoscaler_test.go` | 204 | `36ce313778c1e9a23e7c026ebedf74d31d5d98f9` | `4b6e5ca78ed8a933b7d830335124140a2d65d19062865db4771fecbb320ca7c3` | resource and tune-factor table tests |
| `balancer.go` | 254 | `ebd2f75170a0073c2f9044722450738b46f78795` | `36471d0ebc8e8991b02fada57fa1d2ee5875a9506266e1337467385daa4d6967` | task subtask redistribution across eligible nodes and slot capacities |
| `balancer_test.go` | 476 | `ae941994b600a77af824c3a2be0f6f8495c8836b` | `152915587767236059eba18e9d7c278d76d05c7cb9d2ebeab44cb0d834bcd43f` | balancing, dead-node, capacity, and error-path tests |
| `interface.go` | 295 | `b9cd51149db422242d1192be11a21fde452f436f` | `cfef3ed79efd3e022178536fda941e790759792363df1c3409c66caf4bd965fe` | TaskManager/Extension/Cleaner contracts, scheduler/cleaner factories, and test params |
| `main_test.go` | 64 | `14d81f3cf359b8275129be4666d78f2d86e3c944` | `3255c59621ede9d1edaed99400efbd4978effcf823814ad3df70f38f2da23888` | package test bootstrap, TestMain, and scheduler interface adapters |
| `nodes.go` | 201 | `b15341b155d7a90d1d37e9ee77ed97d32f67deef` | `71320fcd8ee475b5e2c333a78d026d08596e695e39687977ac6f1bac49ceeae4` | live-node maintenance, managed-node refresh, scope filtering, and tracing |
| `nodes_test.go` | 191 | `bb2d84d70e800548bd48bcdb9de5898dea5555ab` | `2a1a8711226a4686b80f5432686e308b189b49518b8129879a40e95bcdd1ac22` | live/managed node and scope-filter tests |
| `scheduler.go` | 840 | `6709fb8db7219489bc3b5a39ddca9b6b77a10847` | `ff8561f7050215613f32df672196736c26a634261d412e3f046cfda1aa9fe114` | BaseScheduler lifecycle, state machine, planning/scheduling, rollback, subtasks, and metrics |
| `scheduler_manager.go` | 620 | `c8dd6f861d7a47032f82d7509963b544532ef149` | `dc9f400d5e9e7e109d842510c958c42e86da79b4db72f78f44acaad7fcd37937` | manager loops, scheduler admission, cleanup/history GC, balancing, and worker metrics |
| `scheduler_manager_nokit_test.go` | 689 | `ffac6592177b44a9ce9798abb86df6a2b128deb9` | `a511cb2d99ec521d2a84318b1580be95dc9e69498d5fe877211ee10675deadc0` | manager ordering, cleanup, cross-keyspace runtime, and no-TestKit integration tests |
| `scheduler_manager_test.go` | 121 | `8e560160138261141246288067a53f73ec2a10d8` | `b245ee3eeb5285c02e91aa11b0cb0678c222f00b22cc496dcfccab7a288965ee` | manager cleaner tests with mock stores |
| `scheduler_nokit_test.go` | 947 | `58e7e9f95315895afea55c3ddc59e1d1cafc7592` | `353d6e895cef7e9110c8e45a4e585e41c2bc24da015bc74be1ddc23fc696175d` | BaseScheduler initialization, transitions, retries, auto-pause, refresh, and finish tests |
| `scheduler_test.go` | 543 | `51adb7999322a5a49bd872fec018bb4835920cc7` | `5e999dff934e633088f0e17a1b5b707ccdd37f34a367977085dc59b72167429c` | scheduler/taskkit lifecycle, simple/parallel stages, cancel/pause, and manager-loop tests |
| `slots.go` | 243 | `7362aa53de922831ecf7df55ca233e9898fbb0b4` | `c4a29ca9cf33066dbf41b63fadff67abd76cbc6f8b29ffb3aa850e8d09f4bdd5` | slot/stripe reservation, capacity updates, and eligible-node filtering |
| `slots_test.go` | 267 | `3bc5edae33b5a63ee19b0f2db4da468beea32f62` | `d6166d23b52e17a270e6fc5efde4412795b5e92acbbaa63f7b0680b30f638cb6` | classic/next-gen reservation, update, capacity, and node filtering tests |
| `state_transform.go` | 67 | `762eeedb32fa06f25047bae0a38313c93adfff15` | `e48932a9f8f888447f324616df473044ce07289a6259e20371ecf08325af9d1b` | task-state transition validity table |

The direct package has 103 production declarations, 47 top-level tests, and
25 additional test helpers/adapters. Autoscaling applies size/index/tune-factor
models and kernel limits; slots reserve task stripes and per-node capacity;
nodes maintain live executor membership and scope preference; balancer moves
pending work while preserving running subtasks and max-node limits. The
BaseScheduler state machine handles initialization, pending/running/modifying,
cancel/pause/resume/revert, planning errors, subtask scheduling, previous
metadata/summary, cleanup, and terminal metrics. Manager loops admit tasks,
elect scheduler ownership, maintain node/slot caches, clean finished/history
rows, and collect worker metrics. Interfaces define all storage/session/task
contracts and extension hooks. Failpoints and SQL/mock-store tests cover
cross-keyspace runtime, KV disk-full auto-pause, owner races, retries, cleanup,
node loss, state transitions, and scheduling fairness.

## Rust ownership and parity decision

Rust's `tidb-dxf` crate owns task/subtask/resource/state value types and
`schstatus` data, while `tidb-domain` owns only generic executor identity
helpers. No Rust crate owns the Go scheduler manager loops, BaseScheduler state
machine, autoscaler, slot reservation, balancer, node liveness, SQL task-table
transactions, or extension/failpoint contracts. No Rust-only DXF scheduler
behavior or ignored test was found to remove. Adding a disconnected scheduler
would be speculative, so this complete Go scheduler package remains an
explicit Go-only boundary; the nested generated mock package is audited
separately.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. Because the
package contains failpoints and integration harnesses, the prescribed wrapper
enabled and disabled Go failpoints around the complete suite in the pinned
detached Go-master worktree:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/scheduler -count=1
# PASS
# ok github.com/pingcap/tidb/pkg/dxf/framework/scheduler 21.214s
```

Ready repository gates for this receipt batch are
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

The remaining risk is distributed-runtime compatibility: task state transitions,
slot accounting, owner election, node liveness, backoff, cleanup/history
transactions, and keyspace runtime ownership must remain synchronized with the
Go storage/taskexecutor implementations. Rust has no equivalent scheduler
runtime at this boundary.
