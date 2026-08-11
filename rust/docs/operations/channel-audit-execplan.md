# Certify `pkg/util/channel` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB uses `channel.Clear` during executor shutdown to discard results until every sender closes. The entire Go package is one generic function and has no package-local unit tests, so this audit must preserve that zero-test fact rather than invent a source suite. Completion means the complete Go production/build inventory is pinned, public Go behavior and real call sites are reviewed, Rust contract tests cover the source loop's buffered, later-arriving, blocking, close, owned, and borrowed receiver behavior, and one package receipt is published.

## Progress

- [x] (2026-08-11 16:10Z) Fixed the complete two-file Go inventory; current bytes match `318e82bbb791bfc2c74ecbb4f89666e072e9803b` and the historical accepted snapshot `d9e197f3bee2ad884cb38c62dfe7c335c6680141`.
- [x] (2026-08-11 16:10Z) Confirmed the package has no `doc.go`, `_test.go`, TestMain, benchmark, fuzz target, example, fixture, testdata, build tag, platform variant, generated input, `go:generate`, `go:embed`, or failpoint use.
- [x] (2026-08-11 16:11Z) Ran the Go package and confirmed `[no test files]`; a public probe observed buffered and later values drained, blocking while open, return after close, receive-only acceptance, and nil-channel blocking.
- [x] (2026-08-11 16:12Z) Reviewed all Go callers, the Rust owner/export/contract files, and the absence of Rust production consumers.
- [x] (2026-08-11 16:12Z) Found that the existing Rust contract claims later-value behavior but sends only values buffered before Clear starts.
- [x] (2026-08-11 16:18Z) Extended the existing contract with a value sent after Clear drained and blocked; both focused tests pass.
- [x] (2026-08-11 16:24Z) Added the current package receipt and completed pre-sync Ready validation on base `f658e77cb2ac6263098b16ce0b661e0fa8319fac`.
- [x] (2026-08-11 16:35Z) Confirmed the fetched remote remained at base `f658e77cb2ac6263098b16ce0b661e0fa8319fac` and repeated the complete Ready profile successfully.
- [ ] Push the single linear package commit without force and verify the freshly fetched remote SHA.

## Surprises & Discoveries

- Observation: this Go package intentionally has no unit-test artifacts.
  Evidence: the complete Git tree contains only `channel.go` and `BUILD.bazel`; `go test -list .` and `go test` both report `[no test files]`. Rust contract tests are therefore source-derived executable evidence, not claimed translations of nonexistent Go tests.

- Observation: later-arriving values are part of the source loop contract but were not exercised by Rust.
  Evidence: Go `for range ch` continues receiving until close. The existing Rust test preloads two values, starts Clear, verifies both are dropped and the worker remains blocked, then closes the sender without sending another value.

- Observation: Go channel direction and Rust channel ownership do not have a one-to-one type mapping.
  Evidence: Go accepts both `chan T` and `<-chan T` while excluding `chan<- T`. Rust channel libraries split Sender and Receiver types; the generic `IntoIterator<Item = T>` accepts owned standard receivers and borrowed standard/crossbeam receivers, but also accepts non-channel iterables. The wider generic surface does not change receiver behavior and no Rust consumer depends on additional iterator types.

- Observation: a nil Go receive channel blocks forever, but Rust receivers are non-null values.
  Evidence: the public Go probe leaves `Clear((<-chan int)(nil))` blocked. Rust does not synthesize a nullable receiver; this is an explicit safe-language integration difference.

- Observation: Go has 31 Clear call sites across executor shutdown and cleanup paths.
  Evidence: callers include analyze, distsql, sampling, shuffle, aggregate, sort, union, hash join, and index-merge/join executors. Rust currently has no caller outside the owner and its contract test, so cross-runtime executor integration is not claimed by this leaf package.

## Decision Log

- Decision: Keep the production `clear<T>(impl IntoIterator<Item = T>)` implementation unchanged.
  Rationale: receiver iteration directly preserves drain-until-disconnect behavior for both standard and crossbeam channels. Adding a second Rust channel abstraction or runtime-specific trait would narrow useful native compatibility without a source-observable benefit.
  Date/Author: 2026-08-11 / Codex

- Decision: Extend the existing blocking test with one value sent after Clear is known to have drained the initial buffer.
  Rationale: this exercises the previously asserted but unproven later-value branch while retaining the source's wait-until-close behavior and immediate Rust drop evidence.
  Date/Author: 2026-08-11 / Codex

- Decision: Treat `318e82bbb791bfc2c74ecbb4f89666e072e9803b` as the accepted package pin.
  Rationale: it is the last commit that changed the direct package path, contains exactly both current artifacts, and current bytes are identical. The later historical receipt pin is retained as corroborating seed evidence only.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

Inventory, source-byte verification, the zero-Go-test decision, normal and race-enabled public probes, Go caller review, Rust owner/export/consumer review, coverage-gap fix, receipt, synchronization check, and both Ready profiles are complete. Only non-force publication and remote-SHA verification remain. Production behavior and dependencies are unchanged.

## Context and Orientation

The accepted Go package consists exactly of `pkg/util/channel/channel.go` and `pkg/util/channel/BUILD.bazel`. `Clear[T any, V chan T | <-chan T]` ranges over the supplied receive-capable channel and discards values until close. Its Bazel target is one public `go_library`; there is no `go_test` target.

Rust owns the function in `rust/crates/tidb-util/src/channel.rs`, exports it through `rust/crates/tidb-util/src/lib.rs`, and exercises it through `rust/crates/tidb-util/tests/channel_contract.rs`. `clear_drains_values_and_waits_for_disconnect` uses an owned standard MPSC receiver and drop counters. `clear_accepts_a_borrowed_receive_only_view` uses a borrowed crossbeam receiver and confirms the disconnected empty state remains observable after Clear returns.

## Milestones

The source-oracle milestone proves the package has no Go tests, fixes both production/build artifacts, and probes the exact `for range` lifecycle through the public API. Acceptance is explicit `[no test files]` output plus observed buffer drain, open-channel blocking, later-value drain, close return, receive-only support, and nil-channel blocking.

The parity milestone maps every valid source behavior to the two Rust contract tests and records unrepresentable or intentionally wider type-domain differences. Acceptance is two passing Rust tests, with the first sending a value after Clear has blocked and the second retaining borrowed receiver access after disconnect.

The publication milestone adds a current atomic receipt and this plan, runs the complete Ready profile, synchronizes one commit to current `hparser-integration`, pushes without force, and proves local and freshly fetched remote SHAs match.

## Plan of Work

Modify only `channel_contract.rs` to send a third `CountDrop` after the initial buffer has drained, wait for its drop, and verify Clear remains blocked until the sender closes. Add the semantic receipt and maintain this plan. Do not change the already-correct production function or export.

Run the Go package and public probe without failpoints, focused Rust contract tests, the receipt gate, full `tidb-util`, formatting, all-target Clippy with warnings denied, and repository lint. The diff has no Go, Bazel, module, Rust manifest, or production-code trigger, so `make bazel_prepare` is not required unless synchronization changes the one-package diff.

## Concrete Steps

From repository root, run the Go authority and source probe:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -list . -tags=intest,deadlock ./pkg/util/channel
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -tags=intest,deadlock -count=1 ./pkg/util/channel
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go run /tmp/tidb-channel-go-probe.go

From `rust`, run focused and Ready Rust gates:

    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --test channel_contract
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings

From repository root, validate the receipt and lint recipe:

    git show 3353b29fb^:rust/scripts/semantic-package-gate.py | /Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 - rust/crates/tidb-util/tests/channel.semantic.toml
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint

## Validation and Acceptance

The Go commands must continue to report no test files; no synthetic Go test count may appear in the claim. The public probe must observe all six source lifecycle properties. The Rust contract command must pass exactly two tests, and the blocking test must prove the later-arriving value is dropped before the final sender disconnects.

The receipt must accept exactly one pinned Go package and one unique focused command. Complete owning-crate tests, formatting, all-target Clippy, and repository lint must pass. The final commit may contain only the channel contract, receipt, and this plan. Publication must be one linear non-force update with a matching fresh remote SHA.

## Idempotence and Recovery

All checks are safe to rerun. Both probes live under `/tmp` and never enter the repository; move them to Trash after evidence is recorded. If remote advances, rebase the one package commit and repeat Ready validation. If the later-value test times out, it drops the sender during unwind so the worker can terminate; do not weaken the lifecycle assertions to hide a hang.

## Artifacts and Notes

Initial Go evidence on Go 1.25.10 `darwin/arm64`:

    go test -list and go test: [no test files]
    buffered-drained=true
    open-channel-blocked=true
    later-value-drained=true
    close-unblocks=true
    receive-only-drained=true
    nil-channel-blocked=true

Initial Rust evidence:

    channel_contract: 2 passed, 0 failed, 0 ignored
    standalone rustc probe: borrowed std::sync::mpsc::Receiver implements IntoIterator and compiles

Pre-sync Ready evidence on base `f658e77cb2ac6263098b16ce0b661e0fa8319fac`:

    go test -list and go test: [no test files]
    public Go probe, normal and -race: all 6 lifecycle observations true
    channel_contract: 2 passed, 0 failed, 0 ignored, including the later-arriving value
    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: pass
    complete tidb-util suite: 338 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; no Go, Bazel, module, target-list, Rust manifest, or production-code trigger

Post-sync Ready evidence on the unchanged remote base `f658e77cb2ac6263098b16ce0b661e0fa8319fac`:

    go test -list and go test: [no test files]
    public Go probe, normal and -race: all 6 lifecycle observations true
    channel_contract: 2 passed, 0 failed, 0 ignored, including the later-arriving value
    semantic receipt: 1 package, 1 unique command
    cargo fmt --all --check: pass
    complete tidb-util suite: 338 passed, 0 failed, 1 ignored; integration tests and doctest pass
    cargo clippy -p tidb-util --all-targets -- -D warnings: pass
    make -o tools/bin/revive lint: pass with revive v1.2.1
    make bazel_prepare: not run; the one-package diff has no trigger

Failpoint decision:

    no failpoint, testfailpoint, or Bazel failpoint dependency match

## Interfaces and Dependencies

The public Rust interface remains `tidb_util::channel::clear<T>(channel: impl IntoIterator<Item = T>)`. The package adds no dependency, runtime state, thread, or channel implementation. The Rust tests use existing standard MPSC and crossbeam-channel dependencies only.

Plan revision note: created after the complete current-source inventory, exact-byte and history review, failpoint decision, zero-test Go commands, public Go probe, standalone Rust borrow probe, complete Go caller inventory, Rust owner/export/consumer review, focused Rust baseline, and change-instruction critique.
