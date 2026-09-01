# `pkg/dxf/framework/handle` Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly six tracked artifacts and 1,361 lines. Every
production, test, and Bazel file was read in full in a detached worktree at the
pinned Go commit before this receipt was written. There is no `doc.go`, fixture,
`testdata`, generated source/input, platform-specific variant, benchmark, fuzz
target, or `OWNERS` file.

| artifact | lines | Git blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 77 | `b76a938a46766be2c3dddac1723356bc6f53dd05` | `d7405c697968c7715dad83dcfce6adb83e16109b7820508ddfb5df4f0f24ddd6` | public handle library and 11-shard flaky test target |
| `handle.go` | 487 | `448011af96ce6c7c497bd6aca5ca4e50f17e86ac` | `ceaac9abbd6b2c06b907674efcd88a6b8e8df53704b0f654339bc97cf5d2fdce` | task submission, waiting/cancellation, retry, scope/split defaults, cloud object-store URI, schedule flags/tuning, object-store creation, and metering entrypoints |
| `handle_test.go` | 238 | `d0abaa8e84270115277d4ce9b9352b5d0c343a27` | `e6b6f4d507225371555b6864c97b39772ae6b3da9835127ea0e7ad8da91236c8` | task lifecycle, retry, target scope, cloud URI, and region split behavior tests |
| `status.go` | 251 | `c5cb858b2fb4a9f8a92cec9354c102975eed2559` | `07429f8b72b6c703f26c41a996e4d2fa71edbe764f000fd339bf60b7ae1b5283` | schedule status/active-task/history APIs, node and busy-node summaries, required-node simulation, and TTL flags |
| `status_test.go` | 132 | `83c497baa9ca0feee0c96d7a5d24c3846c7fafcd` | `3772a74e1581cab04ee44f13d8912928be0acf3a17ff9ac0c2f4206873173177` | required-node and ImportInto step special-case tests |
| `status_testkit_test.go` | 176 | `20a2c263d1b062ec7894a2d564f785428750b878` | `c78c8309529b85c4a5ffe53a4e1655ef8d5b8dc1208c7c2e03c6edb76748c5e7` | schedule/node/busy-node, pause-scale-in flag, and tune-factor SQL integration tests |

The package has 31 production function/method declarations and 11 top-level
test functions. Submission rejects duplicate keys across active/history rows,
persists task metadata, notifies schedulers, and exposes wait/cancel/pause/
resume operations with terminal-state error semantics. Retry honors retryable
classification, backoff, metrics, and context cancellation. Scope and region
split defaults differ between classic and next-gen kernels; cloud-sort URIs
normalize prefixes and optionally append PD cluster IDs. Schedule status
combines running/modifying tasks, node CPU/occupancy, owner identity, and TTL
flags. Object-store and row/size metering helpers use the DXF metering schema.
Tests exercise both kernel branches, duplicate/history behavior, retry and
cancel paths, cloud URI/SEM combinations, node accounting, required-node
packing, pause flags, and tune-factor expiry. Failpoints cover submitted-task
and row/size metering hooks plus CPU/disttask setup.

## Rust ownership and parity decision

Rust has no dependency-closed owner for this Go DXF handle layer. The Rust
`tidb-dxf` crate owns generic task/step/resource data but does not expose
SQL-backed task submission/history, scheduler notifications, keyspace/session
transactions, PD-aware object-store URI normalization, metering writes, or
classic/next-gen kernel branching. No Rust-only handle behavior or ignored test
was found to remove. Adding a disconnected Rust facade would be speculative,
so this complete Go package remains an explicit Go-only boundary.

## Validation and risk

Profile: **Ready** for this documentation-only boundary audit. Because the
package contains failpoint hooks, the prescribed wrapper enabled and disabled
Go failpoints around the exact suite in the pinned detached Go-master worktree:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh ./pkg/dxf/framework/handle -count=1
# PASS
# ok github.com/pingcap/tidb/pkg/dxf/framework/handle 5.520s
```

Ready repository gates for this receipt batch are
`cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`,
`make lint`, and `git diff --check`. No Go source, import section, test,
Bazel target, or module dependency changed, so `make bazel_prepare` is not
required. Rust tests and a full workspace build are not run because no Rust
source or owning target changed.

The remaining risk is cross-component contract drift: task-table schemas,
kernel-mode defaults, PD owner identity, object-store prefixes, metering
timestamps, and scheduler state transitions must remain aligned with the
tested Go callers. Rust has no equivalent dependency-closed implementation at
this boundary.
