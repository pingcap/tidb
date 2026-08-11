# Transcreate `pkg/util/promutil` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB's Rust SQL node needs one small, shared authority for creating native Prometheus counters, gauges, histograms, and their labelled vectors, plus default and no-op collector registries. After this package is complete, Rust subsystems can inject the same Factory and Registry boundaries as Go `pkg/util/promutil`, and callers that already register metrics elsewhere can use a registry that deliberately accepts duplicate registrations and always reports successful unregistration.

The acceptance and publication unit is the whole direct Go package inventory: `BUILD.bazel`, `factory.go`, `registry.go`, and `registry_test.go`. One Rust commit will contain the dependency declaration and lock update, module export, production/test module, semantic receipt, and this plan, then be pushed linearly to `hparser-integration`.

## Progress

- [x] (2026-08-11 11:05Z) Confirmed the four-file direct inventory, no `doc.go`, no failpoint use, and exact source identity at commit `318e82bbb791bfc2c74ecbb4f89666e072e9803b`.
- [x] (2026-08-11 11:06Z) Ran authoritative Go `TestNoopRegistry`; it passed unchanged.
- [x] (2026-08-11 11:17Z) Implemented the complete Factory and Registry production surface with native `prometheus 0.14` types.
- [x] (2026-08-11 11:19Z) Ported `TestNoopRegistry`, covered all six Factory methods and the default registry, and added the semantic receipt.
- [x] (2026-08-11 12:10Z) Rebased the one-package commit without conflict onto remote `51669417528b241025fe16b283bce045ac4fbe35` and reran every Ready gate successfully.
- [x] (2026-08-11 12:11Z) Completed package-boundary and diff self-review; the commit remains the seven declared Rust evidence files and no Bazel trigger is present.
- [ ] Publish the validated one-package commit linearly to `hparser-integration` and verify the remote branch SHA.

## Surprises & Discoveries

- Observation: Go Prometheus constructors return collector interfaces even when descriptor construction records an error; Rust Prometheus returns `Result` at construction time.
  Evidence: Go `promutil.Factory` methods have no error result, while `prometheus 0.14` exposes `Counter::with_opts`, `Gauge::with_opts`, and vector/histogram constructors as `prometheus::Result<T>`.

- Observation: the package test's empty `CounterOpts` collector is accepted only because the no-op registry never inspects it. The test intent is registry non-validation and duplicate acceptance, not empty metric names.
  Evidence: `TestNoopRegistry` registers two counters with identical empty options and expects both operations to succeed.

- Observation: an offline lock refresh selected older unrelated WASM packages because the local sparse index did not contain the versions already pinned by the branch.
  Evidence: the first resolution proposed 11 unrelated downgrades. Restoring that generated lockfile and resolving against the online index produced a minimal lock diff: six new packages, 62 added lines, and no changed existing version.

## Decision Log

- Decision: Use the mature `prometheus 0.14` crate with default features disabled.
  Rationale: Go promutil is an adapter over the native Prometheus client, not a metric engine. The Rust package must also wrap the native implementation. This version is already cached locally; protobuf exposition is outside this package and would add an unnecessary dependency.
  Date/Author: 2026-08-11 / Codex

- Decision: Return `prometheus::Result` from all six Rust Factory methods.
  Rationale: Rust Prometheus validates descriptors at construction. Returning the library-owned error preserves diagnostics and avoids introducing panics that Go does not specify, except for the source's separately documented invalid-bucket behavior.
  Date/Author: 2026-08-11 / Codex

- Decision: Model Go `prometheus.Registerer` as an object-safe local Registry trait taking boxed native collectors.
  Rationale: It preserves injection through `Box<dyn Registry>`, maps native registry errors to Go's `bool` unregistration result, and lets the no-op implementation ignore collectors without descriptor validation.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

The complete package implementation and its Ready validation are complete. The Rust surface covers all six source Factory constructors, both Registry implementations, every operation on the source Registerer boundary, and the unchanged Go package's sole test. The commit is ready for linear publication as one package unit.

Correctness risk is low after the authoritative Go test, focused semantic gate, complete owning-crate test, and all-target clippy passes. Compatibility risk is explicit and contained: Rust Factory methods return native `prometheus::Result` values because the Rust client validates descriptors eagerly, whereas Go collectors retain constructor errors internally until collection or registration. Performance risk is low because the implementation delegates to the native Prometheus client and adds no extra synchronization or collection work.

## Context and Orientation

Go `pkg/util/promutil/factory.go` defines six constructors through `Factory`: Counter, CounterVec, Gauge, GaugeVec, Histogram, and HistogramVec. The default implementation delegates directly to `github.com/prometheus/client_golang/prometheus`. `registry.go` aliases the upstream Registerer interface, supplies a no-op implementation, and constructs a fresh default registry. `registry_test.go` proves the no-op implementation accepts duplicate collector descriptors and always reports successful unregistration.

The Rust implementation belongs in `rust/crates/tidb-util/src/promutil/`. It uses native collector and option types from the Rust `prometheus` crate. `rust/crates/tidb-util/src/lib.rs` exports the module. `rust/crates/tidb-util/tests/promutil.semantic.toml` pins the complete Go inventory and the exact Rust evidence.

## Plan of Work

Add `prometheus = { version = "0.14", default-features = false }` to `rust/crates/tidb-util/Cargo.toml` and let Cargo update `rust/Cargo.lock` offline. In `promutil/mod.rs`, re-export the native metric/option types, define Go-shaped CounterOpts and GaugeOpts aliases, implement an object-safe Factory trait and default factory, then define the Registry trait, native default implementation, no-op implementation, and constructors.

In `promutil/tests.rs`, translate `TestNoopRegistry` with valid native collectors that have duplicate descriptors, because Rust rejects invalid descriptors before registry invocation. Add focused assertions exercising every Factory method and the default registry so every production branch has evidence.

## Concrete Steps

Run commands from repository root unless a command changes directory.

    pushd pkg/util/promutil
    go test -run '^TestNoopRegistry$' -tags=intest,deadlock
    popd

Expect `PASS`.

During implementation:

    cd rust
    cargo test -p tidb-util --lib promutil

Before publication:

    python3 rust/scripts/semantic-package-gate.py rust/crates/tidb-util/tests/promutil.semantic.toml
    cd rust && cargo test -p tidb-util
    cd rust && cargo clippy -p tidb-util --all-targets -- -D warnings
    cd rust && cargo fmt --all --check
    make lint

## Validation and Acceptance

The unchanged Go test and its Rust counterpart must pass. The Factory test must construct and use all six metric families. The default registry must reject a duplicate descriptor and return true/false unregistration results through the local trait. The semantic gate must report one package and verify all four Go artifacts against the source pin. The full owning crate, format, clippy, and Ready lint gates must pass before publication.

The Bazel preflight is expected to skip `make bazel_prepare` because only Rust source, Cargo metadata, and this plan change. Confirm this from staged and unstaged diffs rather than assumption.

## Idempotence and Recovery

All tests and lint commands are safe to rerun. Cargo lock generation is deterministic with the pinned dependency and should be run offline from the cached crate. If the remote branch advances, commit the complete package locally, rebase without force, and repeat every Ready gate. The dirty primary worktree remains unrelated and untouched.

## Artifacts and Notes

Initial Go oracle:

    PASS
    ok github.com/pingcap/tidb/pkg/util/promutil 0.651s

The source and current package inventories are both exactly:

    pkg/util/promutil/BUILD.bazel
    pkg/util/promutil/factory.go
    pkg/util/promutil/registry.go
    pkg/util/promutil/registry_test.go

Initial Rust WIP evidence:

    running 3 tests
    test result: ok. 3 passed; 0 failed; 0 ignored; 324 filtered out
    cargo clippy -p tidb-util --lib --locked -- -D warnings: exit 0

The three Rust tests cover the Go no-op registry surface, all six Factory constructors, default-registry duplicate/unregistration behavior, and both `must_register` outcomes.

Pre-rebase Ready evidence:

    Go TestNoopRegistry: PASS
    semantic package gate: 1 packages, 1 unique commands
    tidb-util library: 326 passed; 0 failed; 1 ignored
    integration contracts and doctest: all passed
    cargo fmt --all --check: exit 0
    cargo clippy -p tidb-util --all-targets --locked -- -D warnings: exit 0
    make lint: exit 0

Post-rebase Ready evidence on remote base `51669417528b241025fe16b283bce045ac4fbe35`:

    Go TestNoopRegistry: PASS (0.738s)
    semantic package gate: 1 packages, 1 unique commands
    tidb-util library: 324 passed; 0 failed; 1 ignored
    tidb-util integration contracts: 22 passed; 0 failed
    tidb-util doctest: 1 passed; 0 failed
    cargo fmt --all --check: exit 0
    cargo clippy -p tidb-util --all-targets --locked -- -D warnings: exit 0
    make lint: exit 0

Bazel preflight found no Go, Bazel, or Go module changes, so `make bazel_prepare` is not required. The optional source-size sweep reports only the three pre-existing baseline files; the new module and test are 162 and 107 lines.

## Interfaces and Dependencies

In `rust/crates/tidb-util/src/promutil/mod.rs`, provide object-safe `Factory` and `Registry` traits and:

    pub fn new_default_factory() -> Box<dyn Factory>;
    pub fn new_noop_registry() -> Box<dyn Registry>;
    pub fn new_default_registry() -> Box<dyn Registry>;

`Factory` returns native `prometheus::Result` values. `Registry::register` returns that same result, `must_register` panics on the first error after any prior successful registrations, and `unregister` returns whether the collector existed. No-op methods ignore their inputs and always succeed.

Plan revision note: created after package inventory, source pin, dependency API, failpoint, and Go-oracle preflight; updated after implementation, minimal lock resolution, WIP validation, both complete Ready gates, rebase to the latest observed remote, and final diff review. Only linear publication and remote-SHA verification remain pending.
