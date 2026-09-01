# `pkg/util/memory` Go-master parity audit

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

This receipt records the complete artifact inventory and the bounded digest
identity/cache behavior implemented in the Rust `tidb-util::memory` owner. It
does not claim that the full memory arbitrator is transcreated: the Go package
also owns process-memory discovery, global-arbitrator wiring, tracker state
transitions, platform hooks, and their broad stress/benchmark surface.

## Complete Go package inventory

The package has 14 root artifacts (8 production files, 5 test/benchmark
files, and one Bazel target), with 11,388 lines in the Go-master snapshot:

| artifact | lines | inventory |
| --- | ---: | --- |
| `BUILD.bazel` | 57 | `go_library` and sharded `go_test`; failpoint, runtime, CPU, config, logging, and metrics dependencies |
| `action.go` | 191 | OOM action interfaces, fallback chaining, priorities, logging, and panic actions |
| `arbitrator.go` | 3,281 | arbitration modes, root-pool/task queues, digest cache, quota accounting, memory-risk controller, and runtime state |
| `arbitrator_test.go` | 2,865 | `TestMemArbitratorSwitchMode`, `TestMemArbitrator`, `TestBasicUtils`, `TestBench`, helpers, and list/hash/arbitrator fixtures |
| `bench_test.go` | 42 | allocator benchmark harness |
| `global_arbitrator.go` | 619 | process-global arbitrator lifecycle, soft/work-mode controls, metrics, and persisted state |
| `main_test.go` | 33 | package `TestMain` failpoint/test setup |
| `meminfo.go` | 217 | Linux cgroup/proc memory discovery and test hooks |
| `memstats.go` | 57 | runtime memory-stat sampling cache |
| `pool.go` | 623 | hierarchical resource pools, budgets, reservation/release, and approximate accounting |
| `pool_test.go` | 584 | resource-pool and budget behavior tests |
| `tracker.go` | 1,355 | statement trackers, actions, arbitrator attachment, small/big budget transitions, and kill transport |
| `tracker_test.go` | 1,147 | tracker/action/arbitrator integration tests and race-sensitive transition fixtures |
| `utils.go` | 317 | recycled list, notifier, digest identity, hash helpers, ratios, and runtime statistics |

There are no package fixtures or generated files. `meminfo.go` contains the
platform-sensitive cgroup/proc path; no separate platform variant is present
in this package snapshot.

## Rust owner and bounded parity decision

The Rust owner is `rust/crates/tidb-util/src/memory/`: `action.rs`,
`arbitrator.rs` plus its `arbitrate`, `digest_profile`, `mem_risk`,
`root_pool`, `runtime_stats`, and test modules, `arbitrator_utils.rs`,
`mem_state_recorder.rs`, `pool.rs`, `process.rs`, `tracker.rs`, `mod.rs`, and
the crate's memory tests. The owner already carries the ported arbitrator and
pool behavior, with explicit Rust boundaries for Go runtime/platform hooks.

Go master removed the old Unicode-code-point `HashStr` API and now builds
digest IDs from length-prefixed UTF-8 bytes with `DigestIDBuilder`. It also
defines `InvalidDigestID = 0` and makes digest-cache lookup/update no-ops for
that sentinel. Rust now exports `DigestIDBuilder` and
`INVALID_DIGEST_ID`, removes the Rust-only `hash_str` export, uses the builder
in the arbitrator flow, and applies the sentinel guard before shard access.
Focused regressions cover component-boundary/order separation and verify that
the invalid ID neither reads nor creates a digest profile.

## Validation (Ready profile)

Go failpoint-enabled targeted run (the package imports `failpoint` from
`meminfo.go` and `memstats.go`):

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/util/memory -run 'Test(BasicUtils|MemArbitrator)$' -count=1
PASS
ok github.com/pingcap/tidb/pkg/util/memory 0.443s
```

Rust owner checks:

```text
cargo +nightly-2026-08-22 test --offline --locked -p tidb-util --lib memory::arbitrator_utils::tests::basic_utils -- --test-threads=1
cargo +nightly-2026-08-22 test --offline --locked -p tidb-util --lib memory::arbitrator::tests::full_flow::mem_arbitrator -- --test-threads=1
cargo +nightly-2026-08-22 check --offline --locked -p tidb-util --all-targets
cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
git diff --check
```

`make lint` is the repository Ready gate for this Rust-only batch; it passed
with the bundled Go runtime. No Go source, import block, Bazel file, or module
dependency changed, so `make bazel_prepare` is not required.

## Risks and unverified surfaces

The bounded change is source-compatible at the Rust crate boundary except for
removing a Rust-only `hash_str` export that has no remaining workspace caller.
Digest IDs now intentionally distinguish component boundaries exactly as Go
master does. The full process-memory, cgroup/platform, global-arbitrator,
tracker transition, failpoint, and benchmark surfaces remain outside this
checkpoint; Windows and other unsupported targets were not exercised.
