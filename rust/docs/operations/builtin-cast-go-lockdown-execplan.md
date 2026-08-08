# Lock down `builtin_cast.go` in `tidb-expr`

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while the work proceeds. It follows `PLANS.md` at the repository root.

## Purpose / Big Picture

`pkg/expression/builtin_cast.go` is TiDB's scalar CAST implementation and is authoritative for value conversion, warning/error behavior, result metadata, and source-type dispatch. This unit creates a source-owned seed receipt in `rust/crates/tidb-expr`: every production AST obligation and every direct scalar-test or benchmark/support obligation receives exactly one PORTED, DECLINED, or UNREACHABLE verdict, PORTED symbols are compile-anchored, and source or test drift makes the gate fail. This is deliberately a file lockdown, not a claim that the whole Go `pkg/expression` package has been transcreated.

No oracle ratchet is required to move. An honest zero-movement or falsification result is successful evidence.

## Progress

- [x] (2026-08-08) Verified `origin` and `ngaut` `hparser-integration` at accepted SHA `842867801eaddcffc25e4de15aabb391f02b1968`.
- [x] (2026-08-08) Created fresh branch/worktree `codex/task325-tidb-expr-builtin-cast-lockdown` and pushed the untouched base to both remotes.
- [x] (2026-08-08) Rejected the stale L1 branch as an integration unit and inspected its four commits only through immutable Git objects.
- [x] (2026-08-08) Measured the accepted source at 1,265 production AST obligations and the direct scalar test/benchmark files at 1,922 obligations.
- [x] (2026-08-08) Classified all 3,187 obligations and added exact source/hash/AST/verdict/symbol gates: 631 PORTED, 2,264 DECLINED, 292 UNREACHABLE.
- [x] (2026-08-08) Corrected stale assumptions for JSON and vector sources; measured and reverted TIME plus native public DATE/DATETIME exposure because their differential integration owner is outside this crate.
- [x] (2026-08-08) Ran 12 boundary/falsification probes: seven killed, one helper-only probe initially survived and was killed after strengthening, and four temporal integration probes were reverted to explicit DECLINED verdicts.
- [x] (2026-08-08) Ran the lockdown gate, formatting, all `tidb-expr` targets, strict Clippy, `git diff --check`, direct ratchet grep (0/100/1/78), and `make -j12 lint` (exit zero with its known Darwin/internal-package diagnostics).
- [x] (2026-08-08) Committed locally and passed the clean detached full-workspace gate. Publication remains coordinator-only; this unit will return the exact local SHA without pushing and reclaim only its own worktree/targets.

## Surprises & Discoveries

- Observation: the package-wide Go inventory command aborts on an unrelated duplicate obligation elsewhere in `pkg/expression` before a caller can filter to the owning files.
  Evidence: `go_package_lockdown_inventory --package pkg/expression` reports duplicate id `O31b9eb5be01200a4`. Running the same unchanged tool against a temporary package root containing only `builtin_cast.go`, `builtin_cast_test.go`, and `builtin_cast_bench_test.go` yields a deterministic 1,265/1,900/22 census.

- Observation: L1's claim covered only 151 top-level production functions. It did not inventory production branches/declarations or any direct Go test/support obligation.
  Evidence: its checked-in `cast_inventory.rs` describes a grep-derived function list; the accepted Go-AST census is 3,187 obligations.

- Observation: accepted-tip `tidb-datatype` already has native `Datum::Time`, `Datum::Duration`, `Datum::Json`, and `Datum::VectorFloat32` domains.
  Evidence: L1's blanket domain-based verdicts were stale. JSON now reaches
  native CAST, while public temporal results still have an integration-owned
  string boundary and vector sources are accepted only for string targets.

- Observation: the first DateTime-FSP mutation survived because its test called a private helper instead of public CAST dispatch.
  Evidence: forcing dispatch FSP to zero left the first version green; the strengthened dispatch assertion kills it.

- Observation: exposing `CAST AS TIME` makes the clean workspace gate fail intentionally.
  Evidence: `difftest-result-tests::expr_corpus_holdouts::cast_as_time_operand_is_still_unevaluated` requires moving the row and regenerating its golden. That owner is outside `tidb-expr`, so this unit reverted the exposure and records a measured DECLINED boundary.

- Observation: exposing native DATE/DATETIME values also fails the clean
  differential gate, and forcing public FSP onto internal wrappers strips
  `convert_tz` fractions.
  Evidence: the exact-SHA workspace gate reported `STR` versus `TIME` for
  three public CAST rows and lost `.25`/`.123456` on two `convert_tz` rows.
  Public temporal results were restored to strings, while public target FSP is
  retained and internal wrappers infer source FSP.

## Decision Log

- Decision: use a temporary isolated package root only as input to the repository's unchanged Go-AST inventory tool.
  Rationale: it preserves real Go parsing and stable repository-relative paths while avoiding an unrelated package-wide duplicate; the checked gate copies exact source bytes and verifies their hashes first.
  Date/Author: 2026-08-08 / Codex.

- Decision: transplant no L1 commit wholesale.
  Rationale: L1 changes `tidb-ast`, `tidb-session`, and differential crates outside this unit, predates the accepted parent, and omits branch/test completeness. Source-specific ideas will be re-proven and manually re-expressed only inside `tidb-expr`.
  Date/Author: 2026-08-08 / Codex.

## Outcomes & Retrospective

The inventory is closed, but it falsifies full `builtin_cast.go` parity: TIME
and native public DATE/DATETIME integration, union-specific
negative-to-unsigned behavior, ARRAY functional-index construction, several
result metadata helpers, the mutable hybrid control-tree rewrite, and Go
chunk-vector execution remain explicit DECLINED obligations. This is a
successful lockdown/falsification receipt, not a parity claim. It fixes
reachable DateTime-FSP parsing, native JSON values/result metadata, and
vector-source rejection.

The failpoint-safe Go oracle command was attempted twice. Both runs restored failpoints to refcount zero but the local arm64 Go linker crashed in `cmd/link/internal/loader.SetSymSect`; no Go test assertion ran. The Rust Ready checks and clean detached `cargo test --offline --locked -j12 --workspace` passed.

## Context and Orientation

The owning Go production file is `pkg/expression/builtin_cast.go`. Its direct scalar behavior tests and benchmark support are `pkg/expression/builtin_cast_test.go` and `pkg/expression/builtin_cast_bench_test.go`. The vector implementation and its tests are a sibling source owner and are not classified here. The Rust owner is `rust/crates/tidb-expr`, principally `src/cast.rs`, with CAST dispatch seams in `src/rewriter.rs`, `src/scalar_function.rs`, and JSON conversion in `src/builtin_ext/json/value.rs`.

The production census is 1,265 obligations: 151 functions, 518 branches, 224 short circuits, 146 switch cases, ten loops, seven closures, 64 declarations, 77 fields, six constants, and 62 variables. The direct scalar test file has 1,900 obligations; the direct benchmark file has 22. The inventory gives every row a source path, AST anchor, node hash, owning function, verdict, Rust symbol or dash, and evidence id.

## Plan of Work

First, build a deterministic checker that copies the three exact Go files into a temporary isolated package root and runs the repository Go-AST inventory tool. It verifies SHA-256, byte/line counts, category totals, unique identities, and byte-for-byte equality with the checked ledger.

Second, audit the production file in source order. Clone-only Go object methods may be UNREACHABLE only with proof that Rust's enum/value representation has no corresponding mutable signature object. Parser- or type-domain exclusions such as vector and multi-valued-index ARRAY casts require construction-boundary proof. Reachable conversion behavior maps to actual `tidb-expr` functions and boundary tests. Mixed Go functions receive branch-specific overrides rather than a broad owner verdict.

Third, classify every direct test and benchmark/support obligation. A Go-only harness object can be UNREACHABLE only where its construction seam truly has no Rust analogue; behavior rows that exercise reachable conversions must point to Rust boundary receipts or to a measured DECLINED refusal.

Fourth, compile-anchor the exact unique PORTED symbol set and gate every evidence row. Mutation probes alter production conditions or dispatch boundaries, never recorded answers. A surviving mutation causes the boundary test to be strengthened and rerun.

Finally, use the Ready profile: exact failpoint decision and targeted Go oracles, all `tidb-expr` targets, formatting, strict Clippy, checker, diff, `make -j12 lint`, then a clean detached full workspace `cargo test --offline --locked -j12 --workspace` without `--all-targets`. Verify ratchets 0/100/1/78, return the local SHA to the coordinator without pushing, and reclaim only this unit's paths.

## Concrete Steps

All commands run from the isolated worktree root unless stated otherwise.

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-tidb-expr-builtin-cast-lockdown cargo test --manifest-path rust/Cargo.toml --locked -j12 -p tidb-expr --all-targets
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-tidb-expr-builtin-cast-lockdown cargo clippy --manifest-path rust/Cargo.toml --locked -j12 -p tidb-expr --all-targets -- -D warnings
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    python3 rust/scripts/builtin-cast-lockdown.py
    git diff --check
    make -j12 lint

The final detached worktree uses a separate target directory and runs:

    CARGO_BUILD_JOBS=12 cargo test --manifest-path rust/Cargo.toml --offline --locked -j12 --workspace

## Validation and Acceptance

Acceptance requires exactly 1,265 production and 1,922 direct test/support obligations with one verdict each; exact source/test hashes and AST identities; no TODO or fallback classification; exact PORTED-symbol/compile-anchor equality; measured DECLINED boundaries; structural UNREACHABLE proofs; killed/restored mutations for every independent reachable rule; scoped and full-workspace gates; ratchets 0/100/1/78; and reclaimed unit paths. Remote publication belongs to the coordinator only.

## Idempotence and Recovery

The checker and tests are repeatable. The temporary AST package root is newly created and removed on every run. Mutations are applied one at a time and restored byte-for-byte before the next. No command edits, cleans, rebases, or deletes the stale L1 worktree. Remote publication is non-force and targets only this task branch.

## Artifacts and Notes

The planned checked artifacts live beside `src/cast.rs` with the `builtin_cast` basename, plus `rust/scripts/builtin-cast-lockdown.py` and this ExecPlan.

## Interfaces and Dependencies

The gate may invoke `go`, `python3`, and the repository's `go_package_lockdown_inventory` source, but adds no external dependency. Rust behavior remains within `tidb-expr`; no other Rust crate is edited. `pkg/expression/builtin_cast_vec.go`, `pkg/executor/distsql.go`, and `hparser-integration` remain outside this unit.
