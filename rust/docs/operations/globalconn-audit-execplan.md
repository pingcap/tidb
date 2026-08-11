# Align and certify `pkg/util/globalconn` as one Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's global-connection package produces cluster-unique connection IDs and recycles the local 32-bit ID space through a concurrent ring. Rust's SQL server uses this package for every MySQL handshake and releases IDs when sessions end. Completion means every source artifact and all nine Go tests are accounted for, public arithmetic and panic boundaries match, the real 20-bit allocator upgrades and downgrades correctly, and live server consumers prove that packed IDs reach clients and remain parseable.

## Progress

- [x] (2026-08-12) Fixed the complete five-artifact Go inventory and accepted source pin `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`; current package bytes match the pin.
- [x] (2026-08-12) Confirmed there is no `doc.go`, generated input, fixture, testdata, platform file, fuzz target, example, `go:generate`, `go:embed`, or failpoint use; inventoried two Go benchmarks and the ldflag integration variant.
- [x] (2026-08-12) Read all Go source, tests, Bazel metadata, both Rust owner modules, historical claims, and live `tidb-server` consumers.
- [x] (2026-08-12) Ran the nine Go tests normally and under race and the ten Rust owner tests.
- [x] (2026-08-12) Probed public zero values, wrapping, truncation, parse/panic boundaries, and a complete 20-bit allocator cycle.
- [x] (2026-08-12) Proved the old Rust zero-capacity failure, replaced non-source subtraction with one wrapping ring mask, and passed the regression.
- [x] (2026-08-12) Finished the public contract, executed both Go benchmarks once, and validated both live server consumers.
- [x] (2026-08-12) Completed the compact receipt, Ready validation, and staged-diff self-review.
- [ ] Rebase one package commit onto a fresh target tip, repeat Ready if needed, push without force, and verify the fresh remote SHA.

## Surprises & Discoveries

- Observation: Go deliberately lets the zero-capacity circular pool initialize by wrapping `uint32(0)-1`, while Rust debug arithmetic panicked during initialization.
  Evidence: the Go probe printed `zero-ring cap=4294967295 len=0 get=(18446744073709551615,false)`; the old Rust regression failed at `pool.rs` with `attempt to subtract with overflow`.

- Observation: a failed zero-capacity Go `Put` claims one tail position before its empty-slot index panic.
  Evidence: the probe printed `put-panic=runtime error: index out of range [0] with length 0 post-put-len=1`. The Rust contract preserves this state transition rather than only checking that both languages panic.

- Observation: Go's integration build mutates the 32-bit server and local widths with linker flags solely to make exhaustion practical.
  Evidence: `tests/globalkilltest/Makefile` sets server bits to 2 and local bits to 4 for `TestServerIDUpgradeAndDowngrade` and `TestConnIDUpgradeAndDowngrade`. The real Rust 20-bit local cycle completed in under one tenth of a second, so no test-only production configurability is necessary.

- Observation: Go truncates both a ring value and the `IDPool.Init` size to `uint32`.
  Evidence: the probe returned value `7` after putting `1<<32+7`, and initializing with `1<<32+1` created the source's size-one ring.

- Observation: the shared audit Cargo target filled the filesystem while compiling the first server consumer.
  Evidence: rustc reported `No space left on device`; `/tmp/tidb-package-audit.DnxFlT/rust/target/debug/incremental` alone occupied 8.9 GiB even though every audit command sets `CARGO_INCREMENTAL=0`. Removing only that regenerable cache restored 9.0 GiB and both consumer tests then passed.

## Decision Log

- Decision: Use `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f` as the complete Go package pin.
  Rationale: it is the existing branch package-ledger pin, is an ancestor of the target branch, contains exactly the same five direct package artifacts, and every current byte matches it.
  Date/Author: 2026-08-12 / Codex

- Decision: Centralize Go's `cap-1` arithmetic in `LockFreeCircularPool::ring_mask` using `wrapping_sub`.
  Rationale: rejecting zero would alter the public source contract, while scattered wrapping calls would leave index and full-state calculations vulnerable to drift. One helper reproduces Go's `uint32` arithmetic everywhere without changing normal power-of-two behavior.
  Date/Author: 2026-08-12 / Codex

- Decision: Cover the ldflag build variant with real-width Rust tests, not configurable Rust production constants.
  Rationale: the Go flags exist only to shrink integration-test runtime. Exhausting all 1,048,575 real 32-bit local IDs is cheap, directly tests the shipped constants, and avoids a test-only production branch. A dynamic getter test separately covers a server ID moving above and below the 11-bit boundary.
  Date/Author: 2026-08-12 / Codex

- Decision: Treat negative AutoInc retry counts, nil callbacks, mutex poisoning, and data-race-free diagnostic reads as native Rust boundaries.
  Rationale: Rust uses `usize`, typed callable values, poison-aware mutexes, and atomics. All representable source outputs and live-consumer behavior remain covered; reproducing invalid or racy Go states would weaken the Rust interface.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

The package inventory, source pin, all owner reads, baselines, public probe, old-implementation failure, minimal arithmetic correction, four-test public contract, benchmark execution, live-consumer validation, receipt, Ready validation, and staged-diff self-review are complete. Publication and fresh-remote verification remain.

## Context and Orientation

The accepted Go package is exactly `pkg/util/globalconn/BUILD.bazel`, `globalconn.go`, `globalconn_test.go`, `pool.go`, and `pool_test.go`. `GCID` packs either an 11-bit server plus 20-bit local ID into a 32-bit-compatible value or a 22-bit server plus 40-bit local ID into a signed-64-bit-compatible value. `GlobalAllocator` starts with a filled 32-bit ring, falls back to an auto-increment 64-bit pool when the ring is exhausted or the server ID is wide, and returns to the ring after enough 32-bit IDs are released.

Rust owns the package in `rust/crates/tidb-util/src/globalconn/mod.rs` and `pool.rs`. `rust/crates/tidb-server/src/sql_node.rs` is the production consumer. The server's `ConnectionTracker` allocates one ID per accepted MySQL connection and releases it through the connection guard. Source-oriented TCP tests in `concurrent_mysql_sessions_source.rs` and `pipeline_mysql_client_source.rs` inspect the handshake ID, parse it, and exercise process-list and kill paths.

The nine Go unit tests cover packing, parsing, reserved IDs, auto-increment exhaustion, ring initialization and FIFO behavior, head/tail overflow, a lock-based reference queue, and five concurrency shapes. `BenchmarkLocalConnIDAllocator` and `BenchmarkPoolConcurrency` are original support artifacts and must at least compile and execute once. `tests/globalkilltest` is an external integration consumer; its width-switch behaviors are reproduced at the real Rust widths while the targeted Rust TCP consumers prove server integration.

## Milestones

The source milestone fixes the five accepted artifacts, nine tests, two benchmarks, ldflag integration variant, two Rust owner files, and production/server-test consumers. It passes Go normal/race and Rust owner baselines before any production edit.

The parity milestone records public Go edge outputs. A regression must fail on the previous Rust implementation before any correction. Public tests then cover zero-capacity ring state, AutoInc zero/reinitialization, parse errors, `u64` truncation, current server-width selection, and full real-width allocator upgrade/downgrade.

The integration milestone executes the two source-oriented TCP consumers named in the receipt and checks the complete `tidb-server` target. The Go benchmarks execute once so their support code and concurrency harness remain build-valid.

The publication milestone adds a compact receipt, completes Ready, fetches the target branch with an explicit refspec, rebases the single package commit if needed, repeats Ready after a rebase, pushes normally, and verifies local, remote-tracking, and `ls-remote` SHAs.

## Plan of Work

Keep the production correction confined to the ring's `uint32` index mask. Keep external public contracts in `rust/crates/tidb-util/tests/globalconn_contract.rs`; do not duplicate all private owner cases. Add `rust/crates/tidb-util/tests/globalconn.semantic.toml` that binds both owner files, the public contract, the server owner, and both TCP consumer tests to the accepted Go pin. Do not change Go, Bazel, Cargo manifests, optimizer, or transaction code.

## Concrete Steps

From repository root, run the Go authority, benchmark support, and race gates:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/globalconn
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^Test' -tags=intest,deadlock -count=1 ./pkg/util/globalconn
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^Test' -tags=intest,deadlock -count=1 ./pkg/util/globalconn
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^$' -bench '^(BenchmarkLocalConnIDAllocator|BenchmarkPoolConcurrency)$' -benchtime=1x -count=1 -tags=intest,deadlock ./pkg/util/globalconn

From `rust`, use `CARGO_INCREMENTAL=0` and the shared `CARGO_TARGET_DIR`:

    cargo test --offline --locked -j12 -p tidb-util --lib 'globalconn::'
    cargo test --offline --locked -j12 -p tidb-util --test globalconn_contract
    cargo test --offline --locked -j12 -p tidb-server --test all 'concurrent_mysql_sessions_source::fixed_workers_hold_three_authenticated_sessions_concurrently_and_drain_all' -- --exact
    cargo test --offline --locked -j12 -p tidb-server --test all 'pipeline_mysql_client_source::mysql_client_reads_the_process_list_and_kills_by_id' -- --exact
    cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings
    cargo clippy --offline --locked -j12 --no-deps -p tidb-server --all-targets -- -D warnings

From repository root, run the semantic package gate, repository lint, and atomic-diff checks. The gate script is recovered read-only from `3353b29fb^` because the target branch removed the gate runner but retained compact receipts.

## Validation and Acceptance

Go must list exactly nine tests and two benchmarks; normal, race, and one-iteration benchmark runs must pass. The public regression must fail before and pass after the wrapping correction. Rust must pass all owner and public tests, both targeted TCP consumers, the complete owning crate, fmt, owner/server Clippy, semantic gate, repository lint, and diff checks. The accepted five Go artifacts must remain byte-identical to the pin. Publication is accepted only after a normal push and a fresh explicit fetch show all three remote/local SHAs equal.

## Idempotence and Recovery

All tests and probes are safe to rerun. The Go probe lives only under `/tmp` and must be moved to Trash after its evidence is captured. Cargo uses a shared target directory with incremental compilation disabled; do not clean it wholesale. If the filesystem fills, only its exact `debug/incremental` cache is safe to remove because these commands cannot reuse it. The clone tracks only `origin/master` by default, so fetch the target with an explicit refspec. If the remote advances, rebase and repeat Ready. Never force push.

## Artifacts and Notes

Failpoint decision:

    No accepted package artifact imports or invokes failpoints, so ordinary targeted Go tests are correct.

Build metadata decision:

    make bazel_prepare is not required: the intended diff changes only Rust production/tests, one receipt, and this plan. It does not add or edit Go/Bazel/module/Cargo-manifest files or add a Go test.

Ready evidence:

    Go listed exactly 9 tests and 2 benchmarks; normal and race tests passed.
    Both benchmarks and every sub-benchmark executed successfully with benchtime=1x.
    Rust owner tests: 10 passed; public contract: 4 passed.
    Both targeted tidb-server TCP consumers passed.
    Complete tidb-util: 346 passed, 1 existing ignored; all integration and doc tests passed.
    cargo fmt, tidb-util Clippy, no-deps tidb-server Clippy, the five-command semantic package gate, and make lint passed.
    The accepted Go package remains exactly five artifacts and byte-identical to the source pin.
    The staged diff contains one private production arithmetic helper, its public regression/variant contract, the compact receipt, and this plan; all source cap-minus-one paths use the helper.

The temporary public probe was moved to the user's Trash as `tidb-globalconn-probe-20260812.go`.

The full Go integration harness under `tests/globalkilltest` requires external PD and TiKV binaries and is not the package-local unit gate. Its two width-transition contracts are exercised directly at real widths in Rust; the exact TCP consumers cover allocation, packing, parsing, release, process-list, and kill integration.

## Interfaces and Dependencies

The public Rust interfaces remain `Gcid`, `parse_conn_id`, `Allocator`, `SimpleAllocator`, `GlobalAllocator`, `IdPool`, `AutoIncPool`, and `LockFreeCircularPool`. The only production change is a private `LockFreeCircularPool::ring_mask(&self) -> u32`. No dependency or manifest changes are planned.

Plan revision note: created after full inventory, source pin, owner/consumer reads, baseline tests, public probing, and the fail-before-fix wrapping regression.

Plan revision note (2026-08-12): recorded completion of both benchmark artifacts and server consumers, receipt authoring, and the narrowly scoped cleanup of unused incremental build cache after disk exhaustion.

Plan revision note (2026-08-12): recorded complete Ready evidence and recoverable cleanup of the temporary public probe.

Plan revision note (2026-08-12): recorded staged-diff self-review and narrowed cache-cleanup guidance to the exact unused incremental directory.
