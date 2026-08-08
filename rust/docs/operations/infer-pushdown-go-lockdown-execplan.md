# Lock down `infer_pushdown.go` in `tidb-expr`

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current. It follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/expression/infer_pushdown.go` decides which scalar expressions may execute in TiKV, TiFlash, or TiDB and applies the expression pushdown blacklist. A wrong positive answer can silently change query results; a false negative only loses performance. This unit creates a file-level seed receipt in `tidb-expr`: every production AST obligation and every direct test/support obligation is classified exactly once, source drift and missing Rust symbols fail a checked gate, and every policy rule representable by the Rust scalar descriptor is implemented and mutation-pinned. This does not claim that the whole Go `pkg/expression` package is transcreated.

## Progress

- [x] (2026-08-08) Verified both remotes at accepted SHA `6fa49fb9112c850ffd9861651792cd043b830a8d` and created a fresh local-only branch/worktree.
- [x] (2026-08-08) Verified that no existing lockdown owns `infer_pushdown.go` or `pushdown_catalog.rs`.
- [x] (2026-08-08) Measured 211 production AST obligations and selected the exact direct tests/support distributed across three Go test files.
- [x] (2026-08-08) Added the complete native pure policy surface and boundary tests.
- [x] (2026-08-08) Added the 996-row checked inventory, evidence, 23 killed mutations, and drift/symbol gate.
- [ ] Run Ready validation and a clean detached full workspace gate; commit locally, preserve the branch ref, reclaim this unit's artifacts, and do not push. Scoped Rust tests, strict clippy, checker, diff check, and `make -j12 lint` have passed; the direct Go oracle was attempted but the macOS arm64 Go linker crashed before test execution and failpoints returned to refcount zero.

## Surprises & Discoveries

- Observation: the existing Rust `pushdown_catalog.rs` is not a lockdown of this source and contains only 23 signature-selection rows for a small TiKV subset.
  Evidence: there is no inventory/evidence file naming `infer_pushdown.go`; the Go source also owns TiFlash, blacklist, context, recursion, warning, failpoint, and metadata rules absent from that table.

- Observation: Rust's generated `ScalarFuncSig` is intentionally smaller than the current Go TiPB enum.
  Evidence: the initial compile anchor could not name `PlusReal`; the generated enum contains only the signatures this rewrite currently lowers. The policy descriptor therefore accepts the exact full TiPB signature name, falling back to the local enum name when available.

- Observation: the first clean workspace gate falsified the initial placement, not the policy behavior.
  Evidence: `source_size_ratchet` reported `pushdown_catalog.rs: 2444 lines, over the 2200-line limit`. Moving the complete source-owned block into `infer_pushdown.rs` restored `pushdown_catalog.rs` to 1,655 lines, and the focused ratchet then passed.

## Decision Log

- Decision: keep the source-shaped pure policy descriptor in sibling module `infer_pushdown.rs`, next to the existing signature-lowering catalog.
  Rationale: one native module should own the Go whitelist and blacklist answers. The clean workspace source-size ratchet rejected adding the full source to the already-large `pushdown_catalog.rs`, and the dedicated module makes the Go ownership boundary explicit. Rules requiring Go's `EvalContext`, `PbConverter`, warning handlers, protobuf metadata objects, failpoints, or session variables remain explicit DECLINED obligations until their runtime owners exist in Rust.
  Date/Author: 2026-08-08 / Codex.

- Decision: direct support means the fourteen Go tests that directly call this source's entry points plus their three local helpers.
  Rationale: unrelated tests sharing the same large files belong to sibling Go sources; pinning the full artifact hashes still makes any supporting-file drift visible.
  Date/Author: 2026-08-08 / Codex.

## Outcomes & Retrospective

The exact ledger closes 211 production and 785 direct test/support obligations: 140 PORTED and 856 explicitly DECLINED, with no manufactured UNREACHABLE verdict. Twenty-three semantic boundary mutations were killed. The pure store, blacklist, TiKV, TiFlash, TiDB-union, and enum rules are native; context, recursive Go expression conversion, warning, metadata, global atomic reload, and failpoint runtime remain measured gaps. This is a successful source-file lockdown and falsification receipt, not a whole-package parity claim.

## Context and Orientation

The authoritative production artifact is `pkg/expression/infer_pushdown.go`. Direct support lives in `pkg/expression/expr_to_pb_test.go`, `pkg/expression/scalar_function_test.go`, and `pkg/expression/fts_to_like_test.go`. The Rust owner is `rust/crates/tidb-expr/src/infer_pushdown.rs`; adjacent `pushdown_catalog.rs` remains the smaller signature-lowering subset. A pushdown verdict is conservative: false means evaluate locally; true permits remote evaluation and therefore must reproduce Go exactly.

## Plan of Work

First add pure Rust policy facts for store type, function name, TiPB signature, source/result field types, charset/collation, CONV's hybrid/binary-literal exception, and FTS modifier facts. Port the complete TiKV and TiFlash switch, TiDB union, enum preliminary rule, blacklist masks, and full-name blacklist rule. Keep the existing signature resolver as the subset that can also lower faithfully to TiPB.

Second run the unchanged repository Go AST inventory tool against an isolated directory containing the four pinned Go artifacts. Filter direct support by its exact owner allowlist, classify every retained row, and check in the resulting ledger. The checker verifies hashes, category totals, one verdict per row, evidence codes, PORTED symbol presence, and the mutation receipt.

Finally run source-focused tests, formatting, strict clippy, `make -j12 lint`, direct ratchet grep, and a clean detached `cargo test --offline --locked -j12 --workspace` from an exclusive target directory.

## Concrete Steps

Run from the fresh worktree root:

    python3 rust/scripts/infer-pushdown-lockdown.py
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-expr --all-targets
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> cargo clippy --manifest-path rust/Cargo.toml --offline --locked -j12 -p tidb-expr --all-targets -- -D warnings
    git diff --check
    make -j12 lint

The final detached worktree runs:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<clean-exclusive-target> cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 --workspace

## Validation and Acceptance

Acceptance requires exact source hashes and AST identities, every retained obligation classified once, every PORTED symbol compiled, every representable independent rule killed by a boundary mutation, Ready checks, unchanged ratchets, and the exact final SHA passing the clean workspace gate. Explicit DECLINED and UNREACHABLE rows are successful falsification evidence, not parity claims.

## Idempotence and Recovery

The checker creates and deletes its own temporary isolated Go package. Mutation probes are applied one at a time and restored before the next. No remote ref is changed. The local branch retains the final commit after worktree reclamation.

## Artifacts and Notes

The inventory, evidence, mutation receipt, and compile anchors live beside `infer_pushdown.rs`; the deterministic checker lives in `rust/scripts`.

## Interfaces and Dependencies

The unit uses existing `tidb_datatype::FieldType` and `tidb_proto::tipb::ScalarFuncSig`. It adds no dependency and edits no Rust crate other than `tidb-expr`.
