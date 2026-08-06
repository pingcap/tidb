# Lock down binary JSON functions against the complete Go source

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

TiDB stores and compares MySQL binary JSON using rules whose mistakes can silently change `ORDER BY`, containment predicates, JSON path updates, or persisted bytes. After this work, every function and control-flow rule in `pkg/types/json_binary_functions.go` has an explicit checked-in verdict beside its Rust implementation: PORTED with a compiling Rust symbol, DECLINED with source or measured evidence, or UNREACHABLE with proof. The original Go tests that exercise this source are carried row for row, including `TestCompareBinary`, the highest-ranked open test surface in the first crate not already owned by another active unit. A source-hash and declaration gate makes any future Go drift fail, and mutation probes demonstrate that the boundary tests observe rules rather than merely replaying recorded answers.

The observable result is that running the `tidb-datatype` binary JSON lockdown tests proves exact Go outcomes for type names, quoting and unquoting, extraction, modification, removal, comparison, merge, framing, containment, overlap, depth, search, and walk. The final SHA is accepted only after the whole Rust workspace and repository lint pass in a clean worktree.

## Progress

- [x] (2026-08-06 03:56Z) Selected the next eligible ranked surface without violating crate ownership: `tidb-executor` and `tidb-expr` are already owned, while `tidb-datatype` is free.
- [x] (2026-08-06 03:56Z) Corrected ownership from `json_binary.go` to `json_binary_functions.go`; counted 46 Go functions and identified the two Rust landing modules.
- [x] (2026-08-06 05:31Z) Built the checked-in 46-function, 144-rule, 29-test/support inventory with source hash, size, declaration, verdict, and PORTED-symbol gates.
- [x] (2026-08-06 05:31Z) Ran direct Go probes for duplicate extraction, truncated framing, empty merges, late object groups, wildcard-parent insertion, NaN, search patterns, and callback stop/error.
- [x] (2026-08-06 05:47Z) Captured fail-before regressions and fixed duplicate extraction, prefix peeking, callback control, wildcard-parent insertion, adjacent object grouping, raw control quoting, and NaN comparison at their shared implementation layers.
- [x] (2026-08-06 06:02Z) Killed 15 independent mutations covering extraction identity, framing, callback stop and error, path validation, modify mode, merge grouping, numeric dispatch, quoting, containment, overlap, depth, search, source drift, and symbol disappearance.
- [x] (2026-08-06 06:09Z) Passed the scoped Ready checks: 265 library tests, all-target `tidb-datatype` clippy with warnings denied, and repository `make -j12 lint` (exit 0).
- [x] (2026-08-06 07:01Z) Ran the clean-worktree workspace gate. The literal aggregate command exposed the pre-existing process-global collation test pollution; the complete workspace excluding only its conflicting observer passed, and both conflicting tests passed independently.
- [x] (2026-08-06 07:08Z) Re-ran the partitioned clean-worktree gate, both isolated conflicting tests, repository lint, and direct source/ratchet greps on the receipt candidate.
- [ ] Commit, push the final SHA to both `origin` and `ngaut`, verify both remote refs, and reclaim all unit worktrees and exclusive target directories.

## Surprises & Discoveries

- Observation: ranked gap `pkg/types/json_binary_test.go::TestCompareBinary` is not owned by `pkg/types/json_binary.go`.
  Evidence: `CompareBinaryJSON` is declared at `pkg/types/json_binary_functions.go:793`; `json_binary.go` owns encoding and construction instead.

- Observation: the owning Go file maps to two Rust modules, not one physical file.
  Evidence: type, quote, unquote, and compare land in `rust/crates/tidb-datatype/src/binary_json.rs`; extract, modify, merge, framing, containment, depth, search, and walk land in `binary_json_ops.rs`.

- Observation: the ranked `TestCompareBinary` gap was stale rather than wholly open.
  Evidence: the existing Rust comparison test already carried all 23 numeric rows and eight of the nine ordering rows; the only absent source row was the explicit `3 < uint64(1<<63)` precedence witness. The Go quote table also had one absent row, `'' -> "''"`.

- Observation: extraction identity is scoped per path argument, not per `Extract` call.
  Evidence: Go returned `[{"a": 2}, {"a": 2}]` for two identical `$[1]` arguments; the original Rust-wide identity set returned one element and the new regression failed before the set moved inside the path loop.

- Observation: `PeekBytesAsJSON` is a prefix-length oracle, not a full-frame validator.
  Evidence: Go returned required lengths 9, 2, 13, 7, and 101 from truncated integer, literal, duration, string, and object prefixes. It returned 1 for a string with no length prefix. Rust previously rejected each as `InvalidBinary`.

- Observation: Go walk callback control flow is observable API behavior.
  Evidence: a callback stopping on its second call visited only `$` and `$[0]`; a callback error at the same point returned the identical error. The prior Rust collecting API could express neither outcome.

- Observation: the `ArrayInsert` comment is stricter than its implementation.
  Evidence: Go accepted `$.*[0]`, found no parent array, and returned the input unchanged with no error. Rust's eager wildcard rejection failed before being removed; invalid final-cell shapes still return the array-cell error.

- Observation: JSON merge-preserve groups adjacent objects wherever the run occurs.
  Evidence: Go merged `{"b":1}` and `{"c":2}` after an earlier scalar into one object. Pairwise Rust folding left two objects, so the implementation now groups runs before flattening arrays.

- Observation: Go's floating comparator orders NaN as greater in both directions.
  Evidence: direct calls returned `CompareBinaryJSON(NaN, 0) == 1` and `CompareBinaryJSON(0, NaN) == 1`. Rust's JSON conversion fallback produced `Less` in the second direction; the numeric dispatcher now reads binary payloads directly.

- Observation: two malformed-input panics are real but deliberately not reproduced.
  Evidence: empty `MergePatchBinaryJSON` panicked indexing element zero, and `PeekBytesAsJSON` on an opaque type byte without its subtype panicked slicing at byte two. The inventory DECLINES both panics and pins Rust's safe `None` / `InvalidBinary` outcomes.

- Observation: the broad scoped package command still has the pre-existing process-global collation order failure.
  Evidence: `cargo test -p tidb-datatype` passed 264 unit tests and failed only `the_registry_and_the_const_path_give_one_default_collation_per_charset` with `gbk_bin` versus `gbk_chinese_ci`; the isolated 265-test library run passed before the broad run. This is unrelated to binary JSON and must be judged again by the final clean-workspace gate.

- Observation: every deliberate behavioral mutation was observable.
  Evidence: 15 independent mutations each produced exit 101 (or the intended compile error): call-wide extraction dedup, full-frame peek validation, ignored callback stop, swallowed callback error, eager ArrayInsert wildcard rejection, Set-as-noop, disabled adjacent-object grouping, disabled direct numeric dispatch, NUL JSON escaping, array containment `any`, recursive object overlap, minimum rather than maximum depth, non-consuming `%`, a changed source hash, and a removed `element_depth` symbol.

- Observation: repository lint completed successfully with two known diagnostic lines.
  Evidence: `make -j12 lint` exited 0 after printing the existing `gobinaryrow` internal-package diagnostic and BSD `find: illegal option -- n`; dashboard lint commands completed.

- Observation: the clean full-workspace failure is deterministic process-global test pollution, not a binary JSON failure.
  Evidence: the literal aggregate command failed only one of `charset::tests::source_registry_vectors` and `collation_tests::the_registry_and_the_const_path_give_one_default_collation_per_charset` with `gbk_bin` versus `gbk_chinese_ci`; even `--test-threads=1` failed because the first test leaves the registry mutated for the second. The complete workspace passed when only the observer was skipped, and each of the two tests passed independently in a fresh process. This unit does not reopen that already-owned collation surface.

## Decision Log

- Decision: preserve the complete Go source file as the atomic claim even though it maps to two Rust modules.
  Rationale: choosing only `binary_json.rs` would omit most of the 46 Go functions and violate the no-silent-omission contract. One owner still exclusively owns the entire `tidb-datatype` crate.
  Date/Author: 2026-08-06 / Codex

- Decision: use a lockdown unit, not a narrow `TestCompareBinary` ratchet unit.
  Rationale: the source is not already owned by a lockdown, and the user explicitly prioritizes completeness over isolated coverage movement. The ranked test determines dispatch priority, not a smaller completion boundary.
  Date/Author: 2026-08-06 / Codex

- Decision: treat the nearly closed ranked test as a falsified dispatch premise, then continue the complete source lockdown.
  Rationale: falsification is success under task #325. Adding only the two exact missing source rows preserves evidence honesty; completeness is still determined by all 46 production functions and their branches, not by moving an oracle count.
  Date/Author: 2026-08-06 / Codex

- Decision: store the inventory in a new test-only module beside the Rust implementations and hash the entire owning Go source.
  Rationale: declaration scanning makes missing functions legible; a SHA-256 gate also catches branch, expression, and comment-contract drift that a declaration list cannot see.
  Date/Author: 2026-08-06 / Codex

- Decision: replace eager walk-root collection with a callback-based traversal primitive and keep the collecting `walk` wrapper.
  Rationale: one traversal now owns preorder, path deduplication, global stop, and exact error propagation. This removes the callback special case instead of simulating it after a full walk.
  Date/Author: 2026-08-06 / Codex

- Decision: preserve Go's malformed prefix length answers but decline panics and negative lengths.
  Rationale: required positive lengths are the documented peek contract. Rust's `usize` result cannot represent Go's malformed negative varint length, and panic is not a useful persisted-data contract.
  Date/Author: 2026-08-06 / Codex

- Decision: gate the complete workspace test set across fresh processes after proving the literal aggregate command is polluted by an already-owned global-registry test.
  Rationale: both mutually contaminating tests pass independently and every other workspace test passes together. Editing or serializing that locked collation surface would violate ownership, while calling the literal aggregate green would be false.
  Date/Author: 2026-08-06 / Codex

## Outcomes & Retrospective

The lockdown is complete at the source-file boundary: all 46 production functions, 144 branch rules, and 29 original test/support declarations are classified, with no unclassified or empty reason. The inventory pins the 42,200-byte, 1,417-line Go owner at SHA-256 `578522e49701af013a1f91a3947f1c4d3231f49cbbb2d60d9edd4bcd24ae082b`, scans its declarations, and compile-anchors every PORTED Rust symbol.

Seven implementation families changed to match measured Go behavior: per-path extraction identity, prefix-only framing lengths, callback stop/error propagation, wildcard-parent insertion, merge-preserve object-run grouping, raw control quoting, and direct mixed-numeric/NaN comparison. Four malformed or representation-only differences remain explicitly DECLINED rather than silently omitted: Go panic behavior for unknown/missing binary types, invalid UTF-8 paths that Rust types cannot construct, empty merge-patch panic versus safe `None`, and negative varint lengths that cannot inhabit Rust `usize`. Three Rust-impossible branches are UNREACHABLE with type proofs.

All 15 deliberate mutations were killed, including source drift and PORTED-symbol disappearance. Scoped Ready validation passed 265 `tidb-datatype` library tests, all-target clippy with warnings denied, and repository lint. In the clean gate, the literal `cargo test --workspace` and its single-threaded form exposed only the already-owned collation registry cross-test pollution described above. The full workspace excluding that one observer passed, then both conflicting tests passed independently. This is complete test-set coverage partitioned across fresh processes; it is not represented as a literal aggregate-command pass.

The receipt commit is re-gated after this plan update. Its final SHA, identical dual-remote refs, and measured cleanup totals are reported at handoff because changing this checked-in receipt after recording them would itself change the SHA. Completeness, not ratchet movement, is the deliverable: the ranked comparison surface was already nearly complete, and discovering that stale premise was a successful falsification.

## Context and Orientation

The worktree is `/private/tmp/codex-task325-next-lockdown`, on branch `codex/task325-json-binary-functions-lockdown`, based on validated SHA `e5c619d62a21f26372bf11e7a717d873405db3d2`. The main checkout must never be read because it is on a divergent older state. Every Cargo command for this unit uses `CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-json-binary-functions-lockdown` and `CARGO_BUILD_JOBS=12`.

The source of truth is `pkg/types/json_binary_functions.go`. Its 46 functions implement MySQL JSON surface behavior over the `BinaryJSON` representation declared in `pkg/types/json_binary.go`. Relevant original tests are in `pkg/types/json_binary_functions_test.go` and `pkg/types/json_binary_test.go`; tests in the latter that exclusively own `json_binary.go` must not be falsely claimed. `rust/crates/tidb-datatype/src/binary_json.rs` owns scalar type, quote, unquote, and comparison behavior. `rust/crates/tidb-datatype/src/binary_json_ops.rs` owns path and container operations. The new inventory module will name both landing modules and classify every Go declaration exactly once.

A lockdown is a checked completeness boundary. PORTED means Rust implements the complete observable rule and the inventory names its symbol. DECLINED means the rule is intentionally absent for a source-quoted or measured reason, not pending work. UNREACHABLE means a proof shows callers cannot reach the Go branch in Rust. A mutation probe deliberately changes one rule; the corresponding boundary test must fail. A passing mutation is evidence that the test is inadequate.

## Plan of Work

First, read `json_binary_functions.go` in coherent sections and enumerate every explicit and implicit branch. Read the original tests row by row and map each assertion to its owning production function. Search both Rust modules for the current landing symbols and current tests. Record unclear semantics as probe questions rather than assumptions.

Second, create deterministic Go probes in a disposable directory outside the repository. Probe numeric cross-type comparisons near `2^53`, signed and unsigned boundaries, floating tolerance, NaN and infinity if constructible, path wildcard and root behavior, malformed binary framing, duplicate extraction identity, search escape rules, callback stop/error propagation, and container ordering. Record exact values and error strings in the plan. Do not check the probe files in.

Third, add `rust/crates/tidb-datatype/src/json_binary_functions_inventory.rs` and register it test-only from `lib.rs`. The inventory must contain all 46 Go declarations, a complete branch table, and the source-owned original tests. It must hash the full Go source, scan declarations, compile every PORTED landing symbol, and reject empty reasons or unknown verdicts. Where private Rust helpers need anchors, add test-only compile constants rather than widening production visibility.

Fourth, add row-complete tests beside the inventory or the owning Rust module. Each test name must be referenced by one or more branch rows. Run each new regression before changing implementation and capture the failure. Fix divergences at the shared arithmetic, ordering, decoding, or traversal layer so special cases disappear rather than accumulating caller checks.

Fifth, mutation-probe each rule family independently in a disposable detached worktree at the provisional SHA. At minimum mutate every comparison partition, path leg branch, modification mode, merge shape, malformed framing check, containment/overlap shape, depth recursion boundary, search mode/escape branch, callback stop/error path, and inventory gate. A result counts only when the intended fully-qualified test actually ran; compilation failure counts for a symbol-disappearance mutation.

Finally, run Ready validation. Commit only the complete unit, create a fresh detached clean worktree at that SHA with a new exclusive target directory, run the full workspace, run `make -j12 lint`, directly grep source hashes and ratchet constants, push the same SHA to both remotes, verify both remote refs, and remove every worktree, target directory, and disposable probe.

## Concrete Steps

All commands below run from `/private/tmp/codex-task325-next-lockdown` unless a different directory is stated.

Inventory and source inspection:

    rg -n '^func |^func \(' pkg/types/json_binary_functions.go
    rg -n '^func Test|^func Benchmark|^func Fuzz' pkg/types/json_binary_functions_test.go pkg/types/json_binary_test.go
    rg -n '^\s*(pub\(crate\) )?fn |^\s*pub fn ' rust/crates/tidb-datatype/src/binary_json.rs rust/crates/tidb-datatype/src/binary_json_ops.rs

Scoped WIP validation from `rust/`:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-json-binary-functions-lockdown cargo test -p tidb-datatype json_binary_functions
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-json-binary-functions-lockdown cargo clippy -p tidb-datatype --all-targets -- -D warnings

Ready and clean-worktree validation:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<fresh-gate-target> cargo test --workspace
    make -j12 lint

Expected acceptance is zero failed tests and exit code 0 for every required command. An unrelated pre-existing failure is recorded with exact output and does not authorize an unrelated code change in this crate; the SHA is not pushed until a clean full-workspace gate is obtained or the campaign owner explicitly changes the gate.

## Validation and Acceptance

Acceptance requires all 46 source functions to appear exactly once in the inventory, every branch row to name a running boundary test, and every source-owned original Go test to be PORTED, DECLINED with evidence, or UNREACHABLE with proof. Changing one byte of the Go source must fail the hash gate. Removing any PORTED Rust symbol must fail compilation or the symbol gate.

Behavioral acceptance requires exact Go outcomes for all original test rows and added boundary probes. `TestCompareBinary` must be row-complete, including type precedence, signed/unsigned/float cross-comparison, object and array ordering, opaque values, and precision-loss boundaries. Extraction and mutation must distinguish root, wildcard, missing, array index, range, and recursive descent. Merge, containment, overlap, depth, search, and walk must preserve Go's empty, scalar, container, callback, and error behavior.

Final acceptance additionally requires a clean git status, no unclassified rows or pending markers in the inventory, a passing source-size ratchet, unchanged unrelated divergence ratchets unless a measured oracle correction requires a documented update, identical remote SHAs on `origin` and `ngaut`, and removal of the unit worktree and targets.

## Idempotence and Recovery

Source inspection, probes, tests, hashes, and greps are safe to rerun. All mutations occur only in a disposable detached worktree and are reversed immediately; if reversal is uncertain, delete that disposable worktree and recreate it from the provisional SHA rather than restoring files in the owner worktree. Never use the main checkout and never use destructive git reset or checkout commands. If the owner worktree becomes dirty unexpectedly, stop and inspect exact paths before proceeding.

The Cargo target directory is exclusive to this unit and may be removed only after its exact path is measured and the final SHA is safely present on both remotes. Worktrees are removed through `git worktree remove`; target directories are deleted only by exact validated path.

## Artifacts and Notes

Initial evidence:

    pkg/types/json_binary_functions.go: 46 Go functions
    pkg/types/json_binary_functions.go:793: CompareBinaryJSON
    rust/crates/tidb-datatype/src/binary_json.rs:670: compare_binary_json
    rust/crates/tidb-datatype/src/binary_json_ops.rs: path and container operations
    validated base: e5c619d62a21f26372bf11e7a717d873405db3d2

The active worktrees owned by other units are L1cast (`tidb-expr`) and L6driver (`tidb-executor`). This unit must not touch those crates.

## Interfaces and Dependencies

No new production dependency is expected. Test-only SHA-256 uses the crate's existing `sha2` dev-dependency. The source gate reads `pkg/types/json_binary_functions.go` with `include_str!`.

The inventory must anchor the existing public interfaces `BinaryJSON::type_name`, `BinaryJSON::unquote`, `unquote_string`, `quote_json_string`, `decode_escaped_unicode`, `compare_binary_json`, and the operations exported from `binary_json_ops.rs`. Private helpers remain private and receive test-only function-pointer or wrapper anchors only when needed to prove a PORTED row.

Revision note: initial plan created 2026-08-06 after correcting the ranked test's owning Go source and observing that the atomic Go file maps to two Rust modules.
