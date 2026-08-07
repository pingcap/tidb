# Lock down the complete `pkg/util/queue` Go package in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan implements root `AGENTS.md` non-negotiable 6: the minimum Go-to-Rust completion unit is one complete upstream Go package. All evidence is taken from dedicated worktree `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-345-wave9` at official integration base `b931f16351cbd7b76bceff8477b7afc7e1ba6632`.

## Purpose / Big Picture

After this unit is complete, a reviewer can prove that Rust `tidb_util::queue::Queue` accounts for the entire direct Go package `pkg/util/queue`, not only the four existing Rust tests. The checked package gate will bind the production source, original test, and Bazel build artifact to their exact hashes and will classify every generated Go AST obligation. Named boundary tests will distinguish the Go zero value from `NewQueue(0)`, prove FIFO order across circular wrap and growth, preserve clear/expand behavior, and preserve empty-pop failure.

The observable result is a queue whose representable Rust inputs follow the Go source exactly. Source drift, an omitted artifact, a missing Rust owner, stale mutation evidence, or a changed boundary rule must fail a named gate.

## Progress

- [x] (2026-08-08) Selected the complete leaf package `pkg/util/queue` after fetching official tip `b931f16351`; confirmed no remote `tidb-util` owner and no package `doc.go` or nested `AGENTS.md`.
- [x] (2026-08-08) Read all three package artifacts, the only direct Go consumer in `pkg/executor/join/base_semi_join.go`, and all 216 lines of the existing Rust owner.
- [x] (2026-08-08) Ran the source baseline: Go `TestQueue` passed, four Rust queue tests passed, and the generic Go AST tool generated exactly 50 obligations.
- [x] (2026-08-08) Measured constructor boundaries in Go and found one representable divergence: the zero value accepts its first push, while `NewQueue(0)` and `NewQueue(0).ClearAndExpandIfNeed(0)` panic on the first push; current Rust accepts both.
- [x] (2026-08-08) Added `source_default_and_zero_capacity_constructor_are_distinct`; it failed before the fix because `Queue::new(0).push(7)` returned, then passed after the production change.
- [x] (2026-08-08) Preserved nil-versus-empty construction state in Rust and added independent wrap-growth, clear/expand, and retained-slot tests; all eight queue tests pass.
- [x] (2026-08-08) Prepared the three-artifact manifest, exact 50-row inventory, compiled owner/evidence gate, deterministic checker, and six-suite/21-mutation plan.
- [ ] Check in mutation results and the receipt gate after replay from an immutable provisional commit.
- [ ] Kill every planned mutation from an immutable provisional commit and restore each source byte-for-byte.
- [ ] Run WIP and Ready/full-workspace gates, publish by non-force fast-forward, and verify GitHub attributes every commit to `dbsid`.

## Surprises & Discoveries

- Observation: the existing Rust module claims a complete transcreation but intentionally collapses two source states that Go distinguishes.
  Evidence: `Queue::push` says an empty backing store always grows to one slot and explicitly notes that this removes the Go `NewQueue(0)` panic.
- Observation: Go negative constructor capacity is a source panic, while a negative clear/expand request only clears and retains capacity.
  Evidence: `/tmp/tidb_queue_probe.go` printed `new-negative panic=runtime error: makeslice: len out of range` and `negative-expand cap=2 len=0 empty=true`. Rust's `usize` API cannot represent either negative input, so these are type-boundary evidence rather than a Rust production branch.
- Observation: the only direct Go consumer always constructs with `chunkRows`, reuses the queue with `ClearAndExpandIfNeed`, and alternates pop/push while probing semi joins.
  Evidence: `pkg/executor/join/base_semi_join.go` creates `queue.NewQueue[int](b.chunkRows)`, queues row indexes, and pushes unfinished indexes back after popping them.
- Observation: `Clear` deliberately retains values in the backing slots, while a larger `ClearAndExpandIfNeed` replaces the slice and releases them.
  Evidence: `source_clear_retains_slots_until_overwrite_or_expand` uses drop counters; clear drops zero values, the first overwrite drops one, and expansion drops the two remaining stored values.

## Decision Log

- Decision: use the complete direct package `pkg/util/queue` as the atomic unit and keep ownership in `tidb-util`.
  Rationale: the package has exactly one production file, one original test file, one build artifact, and one existing Rust landing module. This is the smallest honest completion boundary under `AGENTS.md` non-negotiable 6.
  Date/Author: 2026-08-08 / Codex
- Decision: preserve Go's nil-versus-non-nil empty backing state instead of declaring `NewQueue(0)` a safety divergence.
  Rationale: both the zero value and zero-capacity constructor are representable in the public Rust API, the difference is observable on the first push, and Rust can model it without an API change.
  Date/Author: 2026-08-08 / Codex
- Decision: reuse the generic Go AST generator and add a queue-specific deterministic checker rather than refactor the already content-addressed intset checker.
  Rationale: changing shared intset proof code would invalidate an already published receipt. A narrowly scoped queue checker preserves existing evidence while still rejecting missing, duplicate, unknown, or stale classifications.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

No completion outcome is claimed yet. The measured zero-capacity divergence is fixed and the complete inventory gate passes, but mutation replay, its checked receipt, the independent clean-worktree gate, and publication remain unfinished.

## Context and Orientation

`pkg/util/queue/queue.go` implements a generic circular buffer with a backing slice and `head`, `tail`, and `size` indexes. `Push` allocates one slot only for the Go zero value, doubles a full queue, and copies logical FIFO order from `head`. `Pop` panics on empty. `Clear` resets indexes without dropping the backing slice, while `ClearAndExpandIfNeed` grows only when the requested size is larger. `queue_test.go` contains one top-level test with four `t.Run` cases and 21 assertions. `BUILD.bazel` is part of the package boundary even though Cargo builds the Rust landing.

`rust/crates/tidb-util/src/queue.rs` stores `Vec<Option<T>>` because Rust cannot zero-initialize an arbitrary generic `T`. That representation can preserve the source's occupied-slot and retained-capacity behavior, but it currently uses the same empty `Vec` for the Go nil zero value and `make([]T, 0)`. The fix must add only the state needed to distinguish those constructors. There are no Rust consumers outside the module today; the Go semi-join consumer defines the production FIFO/reuse pattern that tests must preserve.

The direct package has no failpoint use, build tags, platform variants, generated code, `go:generate`, `go:embed`, or tracked `testdata`. The package-specific manifest will record these zero classes so later additions cannot disappear from the claim.

## Plan of Work

First, extend the existing Rust queue test module with source-named boundary tests. One test must prove the default queue allocates one slot and returns the pushed value. Another must call `Queue::new(0).push` and expect a panic; run that exact test before changing production and record its failure. Add focused coverage for growth after wrap, clear retaining capacity while resetting logical contents, expansion preserving the requested capacity, and non-expansion preserving the old capacity.

Second, change `Queue<T>` so `Default` represents the source nil backing slice while `Queue::new` represents an allocated slice even when capacity is zero. `push` allocates one slot only from the nil state. The full-capacity path for an allocated zero-length queue must reach the same failure boundary as Go. Keep public method names and signatures stable.

Third, add `queue.artifacts.tsv`, `queue.inventory.tsv`, `queue.mutation-plan.tsv`, a queue-specific Python checker, and a Rust integration gate. The checker regenerates the exact three-artifact manifest and 50 AST rows, gives every row one `PORTED`, `DECLINED`, or `UNREACHABLE` status, and rejects source or classification drift. All current AST rows are expected to be `PORTED`; negative `int` inputs are recorded as measured API-boundary evidence but are not synthetic AST rows. The Rust gate compile-anchors every public method and verifies every named evidence test still exists.

Fourth, commit the restored implementation and evidence before mutation probing. In a disposable detached worktree with its own target directory, mutate one independent rule at a time: nil-state allocation, zero-capacity constructor failure, full growth, circular copy order, pop order/state, clear, conditional expansion, artifact drift, and symbol deletion. Require the named test or gate to fail, restore from an explicit saved byte copy, verify the source hash, and rerun the same test.

Finally, run the `tidb-util` WIP gate, then the repository Ready gate and an independent full-workspace run from a clean detached worktree. The planned diff contains only Rust, TSV, JSON, Python, and Markdown, so `make bazel_prepare` is not triggered unless the actual final diff introduces a Go, Bazel, module, or generated-file change.

## Concrete Steps

Run all commands from `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-345-wave9` unless the command explicitly uses `rust/` as its working directory.

Check source evidence with:

    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 go test -run '^TestQueue$' -tags=intest,deadlock ./pkg/util/queue
    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 go run ./rust/difftests/tools/go_package_lockdown_inventory --root . --package pkg/util/queue
    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 python3 rust/scripts/pkg-queue-lockdown.py

Run Rust checks from `rust/` with the worktree-exclusive target:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-345-wave9/tgt cargo nextest run --offline --locked -p tidb-util -E 'test(/queue::tests/)' --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-345-wave9/tgt cargo nextest run --offline --locked -p tidb-util --no-fail-fast
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-345-wave9/tgt cargo clippy --offline --locked -p tidb-util --all-targets -- -D warnings -A clippy::needless_update
    cargo fmt -p tidb-util -- --check

Ready additionally requires `cargo nextest run --offline --locked --workspace --no-fail-fast`, `cargo fmt --all -- --check`, workspace clippy with warnings denied, `make -j12 lint`, direct ratchet checks, `git diff --check`, and a clean final worktree. Never run `make bazel_lint_changed`; the user did not request it.

## Validation and Acceptance

Acceptance requires all three package artifacts and all 50 generated obligations to regenerate exactly. Every row must have one allowed status and non-empty evidence. Every `PORTED` production owner must compile, the original Go test must pass, and source-backed Rust tests must exercise the zero/default distinction, FIFO order through wrap/growth, clear/expand behavior, capacity, empty state, and empty-pop panic.

Mutation acceptance requires every planned mutation to fail its intended fully qualified test, source-drift checker, or compile gate and requires the restored source to match its baseline SHA-256 after every probe. Package tests, full workspace tests, fmt, clippy, lint, direct counts, remote ref, and GitHub author attribution must all pass before publication is reported.

## Idempotence and Recovery

The Go inventory generator and checker are deterministic. Cargo commands use only this worktree's `tgt/`. Inventory generation may be rerun and reviewed as a normal diff; it must never hand-edit away a missing source row.

Mutation probes start at an immutable provisional commit. Each source is backed up to a unique exact path outside the repository, restored from that copy rather than with Git checkout or reset, compared by SHA-256, and followed by the same named test. The untracked Cargo target is never staged. Disposable worktrees and exact target paths are removed only after the final SHA is recoverable from the official remote.

## Artifacts and Notes

Baseline artifact hashes at `b931f16351` are:

    0ab3a64e1d621b678f056fcf58e3fed825efd36685609945db2e9e2eaf3c97a7  pkg/util/queue/BUILD.bazel
    4bdbdc1f9a50aa149673d4e80612fcdc893f6fbfd426cb452b2f9db9afe46f93  pkg/util/queue/queue.go
    76dbc2ccc2a43f41c5f305e8d961f8389883453ea3e8db3c94056a13f6b5bdf8  pkg/util/queue/queue_test.go

The generic AST categories total 50: eight functions, eight branches, two loop outcomes, one type declaration, four fields, one test, 21 assertions, and five test closures. The baseline Go run passes `TestQueue`; the Rust filter runs and passes four queue tests.

## Interfaces and Dependencies

No public API or third-party dependency change is planned. `Queue<T>::new`, `push`, `pop`, `len`, `is_empty`, `clear`, `clear_and_expand_if_need`, and `cap` remain stable. The internal representation may add a boolean or enum that distinguishes the Go nil zero value from an allocated empty slice. The checker uses the existing standard-library-only Go AST generator, Python standard library, and the `sha2` dev dependency already present in `tidb-util`.

Security extension review: this leaf utility adds no network, authentication, persistence, deployment, IAM, secret, or dependency surface. The change is limited to in-memory queue semantics and source-evidence files.

Revision note: created on 2026-08-08 after selecting the unlocked package at official tip, reading every direct artifact and consumer, running Go/Rust baselines, generating the 50-obligation census, and measuring constructor boundaries in Go.
