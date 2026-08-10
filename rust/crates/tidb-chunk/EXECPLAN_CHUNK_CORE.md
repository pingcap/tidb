# Close the chunk and row core contract

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current as implementation proceeds.

Reference: `PLANS.md` at the repository root. This plan must be maintained according to it.

## Purpose / Big Picture

`pkg/util/chunk/chunk.go` and `row.go` are the public batch and row boundary used by TiDB execution. The Rust crate already preserves most column values, selection semantics, virtual rows, whole-column identity, and batch transforms, but the two accepted production files remain entirely unclassified. Public row/chunk rendering and an independently owned copied row are absent, the exported zero-capacity constant is absent, and a deep copy of a literal zero-value chunk preserves the wrong nil-column state.

After this plan, every observable contract in the two accepted files has an idiomatic Rust owner, every production obligation has a final `PORTED`, evidence-backed `DECLINED`, or `UNREACHABLE` verdict, and direct Rust consumers compile against the resulting surface. This closes two production files, not the whole `pkg/util/chunk` package; the package claim remains incomplete until every other accepted artifact and helper contract is final.

## Progress

- [x] (2026-08-10 03:05Z) Refreshed the isolated worktree to live `origin/hparser-integration` at `887aca8daffc4844b03fe27a04ab839fb5bbd0b2`.
- [x] (2026-08-10 03:08Z) Read accepted `chunk.go`, `row.go`, and `chunk_test.go::TestToString` at source commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f` and mapped the current Rust implementation and direct consumers.
- [x] (2026-08-10 03:10Z) Identified missing row/chunk rendering, missing owned row copy, missing `ZERO_CAPACITY`, and the zero-value `Chunk::copy_construct` initialization bug.
- [x] (2026-08-10 03:12Z) Preserved fail-before evidence: the copy-state regression failed with virtual-row count `3` instead of `0`, and the public contract failed to compile because `ZERO_CAPACITY`, `Row::copy_construct`, and row/chunk rendering did not exist.
- [x] (2026-08-10 03:15Z) Implemented the Rust-native owned-row and byte-authoritative rendering surfaces plus the copy-state root fix.
- [x] (2026-08-10 03:17Z) Added a public contract covering construction/state, row/range/projected appends, selection/materialization/truncation, whole-column identity, packed raw cells, datum conversion, the source text oracle, every valid evaluation type, arbitrary string bytes, selected rows, owned copies, and the zero-capacity/copy-state boundaries; all eight tests and the full `tidb-chunk` crate pass.
- [x] (2026-08-10 04:12Z) Classified all 296 `chunk.go` and `row.go` obligations as 292 PORTED and four structurally UNREACHABLE, killed and independently verified all six semantic mutations, restored the three production files byte-for-byte, and advanced the official checker to the first unrelated `chunk_in_disk.go` obligation.
- [x] (2026-08-10 03:25Z) Corrected `Row::chunk` to return `Option<&Chunk>` so the zero `Row{}` sentinel exposes Go's nil chunk without panicking; migrated every nonempty internal caller to an explicit expectation and kept the public target and strict Clippy green.
- [ ] Classify all 222 `chunk.go` and 74 `row.go` obligations with exact symbols, rules, probes, structural evidence, and killed mutations.
- [ ] Run WIP and Ready gates, merge the latest integration tip, and push the verified checkpoint to both remotes through `hparser-integration` only.

## Surprises & Discoveries

- Observation: Go `Chunk.CopyConstruct` always assigns `make([]*Column, len(c.columns))`; even when `c.columns` is nil and has length zero, the copy has a non-nil empty slice.
  Evidence: accepted `chunk.go:315-323`. Current Rust copies `columns_initialized`, so `Chunk::default().copy_construct().reset()` incorrectly retains virtual rows.

- Observation: Go strings are byte-authoritative, so row/chunk text can contain invalid UTF-8 through string, ENUM, or SET cells.
  Evidence: accepted `Row.ToString` appends cell strings directly into a byte buffer and converts that buffer to a Go string. Rust must return `GoString`, not a lossy UTF-8 `String`.

- Observation: `Row.CopyConstruct` returns a row whose newly allocated chunk survives by Go pointer ownership. A borrowed Rust `Row<'a>` cannot own that lifetime.
  Evidence: accepted joiner clone code stores `j.defaultInner.CopyConstruct()` independently of the source row. An `OwnedRow` containing the copied `Chunk` is the smallest safe equivalent.

- Observation: Go `ZeroCapacity` is a public constant used by multiple executor builders to request first-batch growth from zero.
  Evidence: accepted repository search finds builder, prepared executor, required-row, and utility consumers. Rust currently exports only `INITIAL_CAPACITY`.

- Observation: the original Rust `Row::chunk()` panicked on `Row::empty()`, while accepted Go returns the row's nil `*Chunk` unchanged.
  Evidence: the zero-row sentinel is public and used by iterator end markers. Returning `Option<&Chunk>` is the idiomatic nullable-pointer contract; all nonempty internal call sites are now explicit about their precondition.

## Decision Log

- Decision: return byte-preserving `GoString` from `Row::to_string(&[FieldType])` and `Chunk::to_string(&[FieldType])`.
  Rationale: this preserves the source value domain, including invalid UTF-8, while retaining an idiomatic owned return value. A Rust `String` would silently narrow the public package contract.
  Date/Author: 2026-08-10 / Codex

- Decision: model `Row.CopyConstruct` as `OwnedRow`, with `as_row`, `chunk`, and `into_chunk` accessors.
  Rationale: the source result owns a newly allocated chunk implicitly through Go's heap. Making that ownership explicit eliminates lifetime edge cases without recreating garbage collection or unsafe self-references.
  Date/Author: 2026-08-10 / Codex

- Decision: classify invalid-input panic shapes, nil receiver method calls, raw pointer arithmetic, integer overflow, and allocator growth details only when TiDB behavior depends on them.
  Rationale: the user requires semantic equivalence, not Go runtime or standard-library emulation. Valid row values, byte output, selection, ownership, quota-visible memory, and direct consumers remain mandatory.
  Date/Author: 2026-08-10 / Codex

## Outcomes & Retrospective

The production/API and exact `chunk.go`/`row.go` receipt milestones are complete, while shipping and the rest of the package remain open. The fail-before tests proved one behavioral bug and three missing public surfaces; the implementation passes the focused eight-test contract, all 184 crate unit tests, and strict crate Clippy. Six independent mutation attempts are KILLED and verified, and the official checker now stops at the first unrelated `chunk_in_disk.go` UNCLASSIFIED obligation rather than either core ledger. This is a bounded production checkpoint, not a whole-package completion claim.

## Context and Orientation

The accepted authority is `pkg/util/chunk/chunk.go` and `row.go` at commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`. Rust production code is `rust/crates/tidb-chunk/src/chunk.rs` and `row.rs`. `Chunk` owns column slots and explicit nil-versus-initialized-empty state. `Row<'a>` is a borrowed physical row cursor. `GoString` is the repository's arbitrary-byte Go string representation.

The current semantic package specification is
`rust/crates/tidb-chunk/tests/pkg_util_chunk_lockdown.toml`; its generated
receipt is the adjacent JSON file.

## Plan of Work

First, add a focused regression proving that copying a literal zero-value chunk creates an initialized empty chunk. Run it against the current code and preserve the assertion failure, then set the copied state to initialized and prove reset/renew behavior.

Second, add `OwnedRow` in `row.rs`. `Row::copy_construct` will renew a one-row chunk from the source's physical layout, append this row, and return the owner. Add `as_row`, `chunk`, and `into_chunk` so callers can read or transfer the copied row without unsafe self-references.

Third, add byte-authoritative row rendering. Iterate the source field types and append exactly the source spellings: signed integer text for `ETInt`; direct bytes for strings, ENUM, and SET; stored time kind text; decimal bytes; duration at the declared FSP; JSON and vector text; and Go fixed shortest float text. Nulls render `NULL` and fields are separated by `, `. Chunk rendering concatenates logical rows and appends one newline per row.

Fourth, add a public core contract covering constructors, required rows, incomplete and virtual rows, selection, projected and batch appends, raw-cell append, copy and selected copy, reset/grow/renew, column identity, reconstruct/truncate, typed datum cases, sentinels, row accessors, owned copy, rendering, pool construction, and exported constants. Reuse existing unit and identity tests where they already prove stronger concurrency or ownership behavior; do not duplicate Go allocator or panic mechanics.

Fifth, classify all accepted obligations. Group rows by semantic rule rather than one mutation per AST row. Use structural proofs for safe Rust type/lifetime substitutions and impossible invalid states, and measured probes only for runtime behavior not provable by a deterministic test. Run each current mutation independently, restore exact source bytes, and verify the rule's boundary test fails for the intended semantic reason.

Finally, run the official package checker. It must advance beyond both core ledgers and stop only at the next unrelated incomplete artifact. Run scoped WIP tests while iterating; before shipping run formatting, strict `tidb-chunk` Clippy, the full crate, all direct-dependent checks, workspace all-target checks, `make -j12 lint`, merge current `origin/hparser-integration`, rerun affected gates, and push the same checkpoint to `origin/hparser-integration` and `ngaut/hparser-integration` without force.

## Milestones

Milestone 1, the public semantic surface, is complete when the baseline failures are recorded and `chunk_core_contract` passes with construction/state, append/selection/transform, identity, packed-cell, datum, initialized-empty copy, owned-row, byte-preserving text, and public zero-capacity coverage. The exact command is the focused integration target in `Concrete Steps`; eight passing tests are the acceptance result.

Milestone 2, the two-file receipt, is complete when every row in `ledgers/chunk.go.tsv` and `ledgers/row.go.tsv` has a final verdict, every `PORTED` group is bound to a registered Rust symbol and killed mutation, every adaptation verdict has immutable structural evidence, and the official checker advances beyond these two ledgers. Zero `UNCLASSIFIED` rows in both files is the acceptance result.

Milestone 3, integration, is complete when the Ready profile passes on the commit after merging the latest live `hparser-integration`, the same SHA is present on both allowed remotes, and no `codex/*` branch is created or pushed. A remote race is handled only by another fetch/merge/gate cycle; it is never resolved with a force push.

## Concrete Steps

From `rust/`, the initial fail-before test is:

    cargo test --offline --locked -j12 -p tidb-chunk --test chunk_core_contract copy_construct_of_zero_value_becomes_initialized_empty -- --exact --nocapture

After the public APIs exist, run:

    cargo test --offline --locked -j12 -p tidb-chunk --test chunk_core_contract -- --nocapture

Run the official receipt checker from repository root:

    python3 rust/scripts/go-package-lockdown.py check --spec rust/crates/tidb-chunk/tests/pkg_util_chunk_lockdown.toml

The intermediate expected result is a failure in the next unrelated unclassified ledger, not `chunk.go` or `row.go`.

## Validation and Acceptance

Acceptance requires red-to-green evidence for the copy-state bug and absent APIs; byte-exact rendering including invalid UTF-8; an independently owned copied row that survives source mutation; the complete valid-input chunk/row surface exercised; both production ledgers at zero `UNCLASSIFIED`; all current mutations killed with restored source hashes; direct dependents compiling; and the Ready profile passing on the final merged SHA. No whole-package claim is allowed while another accepted artifact or helper contract remains unclassified.

## Idempotence and Recovery

Tests, receipt checks, and format checks are safe to rerun. Mutation work must save production source bytes under a `mktemp -d` directory, apply one operator at a time, restore and byte-compare before continuing, and never use a destructive reset. If a remote push races, fetch and merge the live integration tip, rerun gates for incoming paths, and retry without force.

## Artifacts and Notes

Initial core ledger counts:

    chunk.go  222 UNCLASSIFIED
    row.go     74 UNCLASSIFIED

The accepted chunk text oracle is:

    1, 1, 1, 0000-00-00, 1\n2, 2, 2, 0000-00-00 00:00:00, 2\n

## Interfaces and Dependencies

The intended additions are:

    pub const ZERO_CAPACITY: usize = 0;

    pub struct OwnedRow {
        chunk: Chunk,
    }

    impl OwnedRow {
        pub fn as_row(&self) -> Row<'_>;
        pub fn chunk(&self) -> &Chunk;
        pub fn into_chunk(self) -> Chunk;
    }

    impl Row<'_> {
        pub fn copy_construct(&self) -> OwnedRow;
        pub fn to_string(&self, field_types: &[FieldType]) -> GoString;
    }

    impl Chunk {
        pub fn to_string(&self, field_types: &[FieldType]) -> GoString;
    }

No new crate dependency is required.

Revision note (2026-08-10 04:12Z): recorded the completed red-to-green API milestone, added the required milestone narrative, registered the public contract in the package-owned file census, closed the nullable `Row::Chunk` boundary, expanded the public anchor to the full valid core surface, assigned every `chunk.go` and `row.go` verdict, and bound six verified mutations plus four typed-unreachable proofs to those verdicts.
