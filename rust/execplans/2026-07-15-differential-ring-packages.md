# Split differential rings into independently buildable packages

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

Parser agents must be able to build and run parser selectors while expression or execution code is being changed by another agent. Before this plan, every integration target belonged to the single `difftest` Cargo package, whose package-wide development dependencies included `tidb-expr` and `tidb-exec`. Cargo therefore compiled those unrelated crates even for a parser-only target such as `selector_security`. The differential harness now remains one shared library, but parser-ring tests and result-ring tests live in separate Cargo packages with only their real dependencies. A user can observe the improvement non-destructively by inspecting `cargo tree` and confirming that the parser test package has no expression/execution dependency.

## Progress

- [x] (2026-07-15) Confirmed that parser-only targets are blocked by package-global `tidb-expr` and `tidb-exec` development dependencies.
- [x] (2026-07-15) Added real `difftest-parser-tests` and `difftest-result-tests` workspace packages and removed the root package's development dependencies.
- [x] (2026-07-15) Moved 18 parser targets, all 101 then-existing selector modules, and three result targets into one physical owning package each; a concurrent source-backed parser wave added selector 102 directly under the new sole owner.
- [x] (2026-07-15) Added `difftest_root()` and migrated every moved corpus consumer away from package-local path arithmetic.
- [x] (2026-07-15) Strengthened topology validation across all three manifests and fixed selector ownership to include four ADMIN modules whose filenames do not end in `_selector.rs`.
- [x] (2026-07-15) Proved dependency isolation, focused and full package tests, combined Clippy, formatting, source ledger stability, reviewed parser counters, and a clean parser-selector rebuild.

## Surprises & Discoveries

- Observation: `cargo test -p difftest --test selector_security` compiles all package development dependencies, not only the crates imported by that target.
  Evidence: during the 2026-07-15 `Datum` migration, parser-only selector and topology targets repeatedly failed in `tidb-expr` before their test process started.

- Observation: before the move, five tests constructed corpus paths from their own `CARGO_MANIFEST_DIR`.
  Evidence: the old `difftests/tests/{lexer_diff,parser_diff,expr_diff,query_diff,table_diff}.rs` files used that environment variable, so the physical move required the shared `difftest_root()` function rather than duplicated `../` arithmetic.

- Observation: the root package's baseline development tree directly owns all four test-only engine crates.
  Evidence: `cargo tree -p difftest --edges dev` listed `tidb-ast`, `tidb-exec`, `tidb-expr`, and `tidb-lexer`; this is the dependency edge the package split must delete.

- Observation: the old topology check counted only files ending in `_selector.rs`, but four ADMIN selector modules use shorter historical names.
  Evidence: the moved tree contains 101 `.rs` selector modules but only 97 `*_selector.rs` files; the new topology check treats every Rust file below `tests/selectors/` as an owned selector.

- Observation: the package boundary provides real build isolation, not only a cleaner dependency graph.
  Evidence: `selector_security` and `difftest_topology` passed in `difftest-parser-tests` while the concurrent ALTER TABLE migration left `tidb-exec` temporarily uncompilable; `cargo tree -p difftest-parser-tests | rg 'tidb-(expr|exec)'` was empty with status 1.

- Observation: `cargo clean -p difftest-parser-tests` removed substantially more cached artifacts than the package's own small binaries suggest.
  Evidence: Cargo reported 48,256 files and 1.4 GiB removed. The required follow-up selector rebuild still compiled only `difftest-parser-tests` and passed all five security tests without building expression or execution crates.

## Decision Log

- Decision: split by differential ring into real Cargo packages, not optional features on one package.
  Rationale: package boundaries make dependency ownership structural and let Cargo schedule independent rings. Feature combinations would retain one overloaded manifest and permit accidental heavy dependencies to leak back into parser targets.
  Date/Author: 2026-07-15 / Codex root.

- Decision: keep the shared library package named `difftest` at `difftests/`.
  Rationale: oracle decoding, corpus validation, inventory tools, and binaries are shared evidence infrastructure. Renaming it would create churn without removing the dependency bottleneck.
  Date/Author: 2026-07-15 / Codex root.

- Decision: create packages named `difftest-parser-tests` and `difftest-result-tests`.
  Rationale: the names state their purpose and avoid colliding with the shared `difftest` library. The parser package owns lexer/parser/static-oracle selectors; the result package owns expression, query, and table differential tests.
  Date/Author: 2026-07-15 / Codex root.

- Decision: keep all generator binaries physically and logically owned by the root `difftest` package; moved inventory/queue/manifest tests invoke those binaries through `cargo run -p difftest`.
  Rationale: registering the same binary source in the parser package would create two Cargo authorities. The root package has no heavy development dependencies after the split, so invoking its evidence tools preserves parser isolation.
  Date/Author: 2026-07-15 / Codex oracle_cache.

## Outcomes & Retrospective

The split is complete. The shared `difftest` package owns two evidence tests and all generator binaries, `difftest-parser-tests` owns 18 explicit integration targets and 102 selectors across 11 shards, and `difftest-result-tests` owns the three result rings. The parser dependency tree contains only `difftest`, lexer, AST, and parser; neither `tidb-expr` nor `tidb-exec` appears. This isolation was also observed during a real concurrent executor compile break, when parser topology and security selectors continued to pass.

All moved rings retained behavior: the reviewed static parser snapshot is 49,217 exact matches, 1,468 parse failures, and 867 restore mismatches; all three result rings pass against their unchanged Go goldens; the ledger remains at 13,658 untriaged, 129 partial, 12 covered, and zero blocked. Combined Clippy with warnings denied and workspace formatting both pass. The only operational tradeoff is that three parser evidence tests launch the root package's single-owner generator binaries through Cargo; this avoids duplicate binary authorities and does not introduce heavy dependencies into the parser package.

## Context and Orientation

The Rust workspace manifest is `rust/Cargo.toml`. The shared differential package is `rust/difftests/Cargo.toml`; it now owns two evidence tests. The parser package owns 18 explicit targets and 102 parser selector modules routed through 11 stable shard entrypoints. The result package owns three explicit targets. The shared library in `rust/difftests/src/lib.rs` exports corpus and parser-oracle helpers. The corpus and coverage evidence remains physically rooted at `rust/difftests/corpus/` so existing tools and generated inventories do not move.

A Cargo development dependency applies to every integration target in its package. This is the bottleneck: the current manifest names `tidb-expr` and `tidb-exec` under `[dev-dependencies]`, so parser targets cannot build independently.

## Plan of Work

First add `difftests/parser-tests` and `difftests/result-tests` as workspace members. The parser package depends on the shared `difftest` library plus `tidb-lexer`, `tidb-ast`, and `tidb-parser`; it must not depend directly or transitively on `tidb-expr` or `tidb-exec`. The result package depends on the shared library plus the AST, parser, expression, and execution crates. Both packages use `autotests = false` and list every root integration target explicitly.

Move the lexer, parser, static-oracle, parser-manifest, topology, and 11 selector shard entrypoints into `difftests/parser-tests/tests/`. Move the complete `selectors/` directory under that package so feature ownership stays next to its shards. Move `expr_diff.rs`, `query_diff.rs`, and `table_diff.rs` into `difftests/result-tests/tests/`. Keep the ledger and plan-inventory evidence tests with the root `difftest` package because they do not execute result semantics. Do not duplicate test source through `#[path]` aliases across packages.

In `difftests/src/lib.rs`, expose a small `difftest_root() -> PathBuf` helper compiled from the shared package's `CARGO_MANIFEST_DIR`. Replace moved tests' package-local environment lookups with this one function. The parser-oracle module's existing repository-root helper remains authoritative for the checked static Go oracle.

Update `difftest_topology` in the parser package. It must scan its own `tests/selectors/**/*.rs`, verify every selector appears in exactly one shard, verify no selector remains at a package test root, and parse the parser package manifest rather than the old root manifest. Add a workspace-level ownership assertion, either in the root evidence test or the topology target, that every `.rs` directly below each package's `tests/` directory is explicitly registered in that package's manifest.

Finally remove `tidb-expr` and `tidb-exec` from the root package's development dependencies. Do not retain an old compatibility target or path; the package split is the only authority.

## Concrete Steps

All commands run from `rust/`.

Inspect the dependency boundary before editing:

    cargo tree -p difftest --edges dev

After moving targets, prove the parser package is isolated:

    cargo tree -p difftest-parser-tests | rg 'tidb-(expr|exec)'

The second command must print nothing and return `rg` status 1 because neither forbidden crate is present. Then run:

    cargo test -j 12 -p difftest-parser-tests --test difftest_topology -q
    cargo test -j 12 -p difftest-parser-tests --test selector_security -q
    cargo test -j 12 -p difftest-parser-tests --test integration_parser_diff -q
    cargo test -j 12 -p difftest-result-tests -q
    cargo test -j 12 -p difftest -q
    cargo clippy -j 12 -p difftest -p difftest-parser-tests -p difftest-result-tests --all-targets -- -D warnings
    cargo fmt --all -- --check
    cargo run -j 12 -q -p difftest --bin go_test_ledger -- --check

## Validation and Acceptance

Acceptance requires more than successful compilation. The parser dependency tree must contain no `tidb-expr` or `tidb-exec`. All 102 current selectors must still be owned exactly once by the 11 parser shards. Static parser counters must remain at their reviewed snapshot unless a separately reviewed parser feature intentionally changes them. Expression, query, and table goldens must produce the same labels as before the physical move. The root ledger check must report no inventory drift.

The strongest non-destructive independence proof is:

    cargo clean -p difftest-parser-tests
    cargo test -j 12 -p difftest-parser-tests --test selector_security -q

Inspect Cargo output and `cargo tree`; it must not build or list the expression or execution crates. Do not deliberately edit or break another crate merely to demonstrate isolation.

## Idempotence and Recovery

Cargo manifest edits, directory moves, formatting, and all validation commands are safe to repeat. Because the outer TiDB worktree currently sees the Rust tree as untracked, do not use destructive Git reset or checkout commands for recovery. If a move is interrupted, compare the three manifests' explicit target lists with `rg --files difftests | sort`; restore one physical owner for every test before running Cargo. Never leave duplicate `#[path]` owners as a temporary compatibility layer.

## Artifacts and Notes

The triggering failure was not a semantic parser failure: parser-only targets stopped during compilation of concurrent `tidb-expr` changes. That evidence distinguishes dependency coupling from test behavior and is why the package split belongs in the structural refactor.

The final dependency proof was:

    cargo tree -p difftest-parser-tests | rg 'tidb-(expr|exec)'
    # no output; status 1

The complete parser tree contains `difftest`, `tidb-lexer`, `tidb-ast`, and `tidb-parser` only. Final behavioral gates passed with the commands in `Concrete Steps`; `cargo test -j 12 -p difftest-parser-tests -q` and the three moved generator checks also passed.

## Interfaces and Dependencies

The shared `difftest` library must export:

    pub fn difftest_root() -> std::path::PathBuf

`difftest-parser-tests` may depend on `difftest`, `tidb-lexer`, `tidb-ast`, and `tidb-parser`. `difftest-result-tests` may depend on `difftest`, `tidb-ast`, `tidb-parser`, `tidb-expr`, and `tidb-exec`. The root `difftest` package must not use `tidb-expr` or `tidb-exec` merely to make unrelated tests discoverable.

Revision note (2026-07-15): Initial plan records the package-global dependency bottleneck discovered during the `Datum` migration and defines the ring-owned package layout.

Revision note (2026-07-15): Implementation added the two ring packages, shared corpus-root API, single physical ownership for every test, and cross-package topology checks; validation evidence remains to be recorded.

Revision note (2026-07-15): Final validation recorded dependency isolation, 102-selector topology, reviewed parser counters, unchanged result goldens, clean rebuild evidence, combined Clippy, formatting, and ledger stability.
