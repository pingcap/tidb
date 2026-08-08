# Lock down `pkg/executor/point_get.go` against one native Rust owner

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while executing it.

Reference: `PLANS.md` at repository root. This plan follows that file and the stricter repository policy in `AGENTS.md`.

## Purpose / Big Picture

After this unit, a reviewer can regenerate an exact abstract-syntax-tree census for Go's `pkg/executor/point_get.go` and its directly coupled tests, see one verdict for every obligation, and compile-test every claimed Rust symbol. Five metadata-only rules that the current native executor can represent live in `rust/crates/tidb-executor/src/point_get.rs`. Snapshot, locking, chunk, checksum-writing, and runtime-stat rules remain explicit measured declines. This is a file-lockdown seed and does not claim the whole `pkg/executor` Go package is transcreated.

## Progress

- [x] (2026-08-08) Verified both `origin` and `ngaut` `hparser-integration` refs at accepted commit `566c460c26d5019fd32ce157531bcf431a8ce447`.
- [x] (2026-08-08) Created and dual-pushed the isolated task branch before edits.
- [x] (2026-08-08) Proved a 763-obligation initial census: 358 production and 405 directly coupled test/support obligations.
- [x] (2026-08-08) Added five source-shaped metadata helpers and six boundary tests in the new Rust owner module.
- [x] (2026-08-08) Checked in the exact inventory, artifact manifest, reconstructible receipt, symbol/drift gate, and 22 killed mutation results.
- [x] (2026-08-08) Passed the scoped Go owners, required RealTiKV owner, Rust all-target suite, strict clippy, formatting, receipt checker, and `make -j12 lint`.
- [x] (2026-08-08) Replayed the full locked Rust workspace in a clean detached worktree at code-bearing candidate `a88e7f5211ac7f8e88e373914f0f20b91dcb1452`; checker, formatting, clean status, and direct ratchets `0/100/1/78` passed. The post-plan receipt commit receives the same clean replay before local delivery.

## Surprises & Discoveries

- Observation: many executor tests contain SQL that the planner may turn into `Point_Get`, but only a small set is directly coupled to this source file.
  Evidence: exact repository search found direct coupling through the dedicated `point_get_test.go`, the two `pointGetRepeatableReadTest` failpoints, `assertPointReplicaOption`, the concrete `PointGetExecutor` type label, and the point-get index-usage reporter assertions. Generic SQL consumers do not name a symbol or failpoint owned by this file and are not classified as direct owners.

- Observation: the existing Rust point-get planner and row-read paths do not make Go's executor-side state machine wholly ported.
  Evidence: `rust/crates/tidb-executor/src/lib.rs` explicitly defers context propagation and runtime statistics; no Rust type exposes Go's pessimistic lock cache, `kv.Snapshot` options, `chunk.Chunk` decoder contract, or row-checksum writer.

- Observation: the first manual status arithmetic undercounted the reachable helper obligations by one.
  Evidence: exact generated validation reported 28 `PORTED`, 734 `DECLINED`, and one `UNREACHABLE` row; the hard-coded checker expectation was corrected before any receipt was accepted.

- Observation: formatting initially invalidated the recorded Rust mutation source hash.
  Evidence: `cargo fmt --check` rejected the pre-format source and the checker then rejected the old plan hash. The source was formatted, all 22 mutations were rerun against SHA-256 `62bf005745295f5f51bebee168cf22ce8fd7d41bd8e76364a9dd88333b8205c9`, and the reconstructed receipt passed.

## Decision Log

- Decision: use one new Rust owner, `rust/crates/tidb-executor/src/point_get.rs`, and do not edit the already owned `driver/access.rs`, `find_best_task` receipt, `distsql` receipt, or batch-point-get files.
  Rationale: overlapping planner terminology is not evidence that executor snapshot/locking semantics are present, and the task forbids reopening those completed units.
  Date/Author: 2026-08-08 / Codex

- Decision: define direct test/support ownership as either the dedicated source test file or an exact source-symbol, failpoint, concrete-type-label, or reporter behavior coupling.
  Rationale: classifying every SQL statement that happens to produce a point plan would make ownership depend on planner choices rather than on this Go source and would silently absorb unrelated test suites.
  Date/Author: 2026-08-08 / Codex

- Decision: port only `GetPhysID`, `matchPartitionNames`, `shouldFillRowChecksum`, `notPKPrefixCol`, and `getColInfoByID` rules that fit current Rust metadata types.
  Rationale: these rules are pure and exact. The remaining owners require interfaces the native executor does not have, so a similarly named wrapper would be a false port.
  Date/Author: 2026-08-08 / Codex

- Decision: do not publish this task branch after the coordinator's user-override message.
  Rationale: only the coordinator may gate and push the final code to `hparser-integration`; this unit returns an exact local commit instead.
  Date/Author: 2026-08-08 / Codex

## Outcomes & Retrospective

The lockdown closes all 763 obligations: 28 are `PORTED`, 734 are explicitly `DECLINED` at measured missing native boundaries, and one is structurally `UNREACHABLE`. Five pure helpers are compiled in Rust, six behavior tests pin their boundaries, three receipt tests pin the ledger, and all 22 mutation variants are killed. This completeness result moves no differential oracle and is still a successful lockdown.

Scoped Go validation passed for seven direct `pkg/executor` owners, four direct `pkg/executor/internal/exec` owners, and RealTiKV `TestStaleReadKVRequest`; each failpoint wrapper restored refcount zero. TiUP services were stopped, PD became unreachable, and the exact `task325-point-get` data directory was reclaimed. Rust `tidb-executor --all-targets` passed 528 library tests (524 passed, four ignored) and all integration targets; strict clippy passed with `-D warnings`. `make -j12 lint` exited zero while printing the repository's existing Darwin `find -name` and Go internal-package diagnostics. A clean detached locked workspace replay passed at code-bearing candidate `a88e7f5211ac7f8e88e373914f0f20b91dcb1452`; the final documentation/receipt commit is replayed after this plan closes and reported in the delivery handoff.

## Context and Orientation

The Go owner is `pkg/executor/point_get.go`; its dedicated test file is `pkg/executor/point_get_test.go`. Direct external owners are restricted to named test functions selected by the static coupling rule above. The native owner is `rust/crates/tidb-executor/src/point_get.rs`; adjacent `.tsv` and `.json` files hold the immutable artifact, inventory, mutation, and receipt evidence. `rust/scripts/pkg-executor-point-get-lockdown.py` regenerates and validates the census through `rust/difftests/tools/go_package_lockdown_inventory` using isolated temporary package roots.

An obligation is one function, declaration, field, branch side, loop boundary, closure, short-circuit side, test owner, assertion, or table row emitted by the repository's Go AST collector. `PORTED` names a compiled Rust symbol. `DECLINED` gives a measured missing-boundary reason. `UNREACHABLE` gives structural proof that the Rust owner cannot express the Go input.

## Plan of Work

Finish the Rust helper tests first so their source hash is stable. Add the adjacent manifest and inventory with source SHA-256, byte, line, AST-anchor, and node-hash evidence. Add a checker that isolates only the accepted source/test artifacts, filters external test files to exact owner functions, regenerates every row, validates compiled-symbol mappings, reconstructs the receipt, and rejects stale mutation source paths or hashes.

Run actual source mutations against every ported helper's boundary suite. Each mutation must make the scoped Rust test fail, and the restored source must pass. Also mutate artifact paths, source hashes, AST source, inventory verdicts, compiled symbols, and receipt counts so each gate boundary is demonstrated rather than merely asserted.

Use the failpoint wrapper for `pkg/executor` because both the source and direct test package contain failpoint calls. Treat the RealTiKV failpoint owner as inventory evidence and run it only through the repository's required RealTiKV lifecycle if the completion contract requires executing that external owner.

## Concrete Steps

From repository root, regenerate and check the receipt:

    python3 rust/scripts/pkg-executor-point-get-lockdown.py

Run scoped tests and formatting with the unit-exclusive target directory:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> cargo test --offline --locked -j12 -p tidb-executor --all-targets
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> cargo clippy --offline --locked -j12 -p tidb-executor --all-targets -- -D warnings
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    make -j12 lint
    git diff --check

From a separate clean detached worktree at the final commit:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<clean-exclusive-target> cargo test --offline --locked -j12 --workspace

## Validation and Acceptance

Acceptance requires byte-exact inventory regeneration, no blank or unknown verdict, a compiling symbol for every `PORTED` row, structural proof for every `UNREACHABLE` row, non-empty measured evidence for every `DECLINED` row, and killed/restored mutation results. The full Ready profile and clean workspace test must pass. Direct greps must report ratchets `0/100/1/78`.

No production parity claim is permitted for transaction, snapshot, lock, decoder, checksum-writing, or runtime-stat behavior while those interfaces remain absent. No movement in a differential oracle is required; completeness and falsification are the deliverable.

## Idempotence and Recovery

The checker and tests are read-only and safe to rerun. Mutation probes copy or restore the exact source bytes and verify the restoration hash. Worktree and Cargo-target cleanup names only the random exact paths created for this unit. The accepted `hparser-integration` ref and unrelated worktrees are never changed.

## Artifacts and Notes

Accepted parent: `566c460c26d5019fd32ce157531bcf431a8ce447` on both remotes. Local task branch: `codex/task325-tidb-executor-point-get-lockdown`; it is not to be updated remotely.

Initial census: 763 obligations total, with 358 production and 405 direct test/support obligations.

## Interfaces and Dependencies

`physical_table_id`, `partition_name_matches`, `row_checksum_column`, `not_primary_prefix_column`, and `column_by_id` use existing `PartitionDef`, `KvColumn`, and `tidb_model::column::EXTRA_ROW_CHECKSUM_ID` types. No dependency is added. The checker uses Python's standard library, Go, Git, and checked-in repository tools.

Revision note (2026-08-08): initial plan records the exact claim boundary, direct-owner rule, five reachable helpers, measured decline boundary, and completion gates.
