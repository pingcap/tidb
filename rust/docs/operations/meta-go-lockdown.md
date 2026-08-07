# Lock down `pkg/meta/meta.go` against `tidb-meta`

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to
that contract.

## Purpose / Big Picture

TiDB stores catalog state, schema versions, global identifiers, DDL history,
bootstrap state, and resource policies under the `m` metadata namespace in
TiKV. Rust already reproduces several key and value codecs, but it cannot run
Go's transactional `Mutator`: the ranked `TestMeta` scenario still has no Rust
equivalent. After this lockdown, a caller can apply every operation owned by
`pkg/meta/meta.go` to a transaction-shaped key/value store and observe the same
keys, values, errors, ordering, increments, deletions, filtering, and malformed
input behavior as Go.

The checked-in inventory beside the Rust implementation will pin the complete
Go source and classify every production declaration and every outcome of every
syntactic control locus. It will also classify the original source-owned tests
and build support, compile-anchor each `PORTED` Rust symbol, and fail on source
drift or an unclassified row. This is one source-file lockdown; it does not
claim the whole `pkg/meta` Go package is transcreated.

## Progress

- [x] (2026-08-07) Verified
  `b5c1aee5678778f68947fcfbfae4901464fe58da` on both remotes as the accepted
  campaign tip and created isolated branch
  `codex/task325-tidb-meta-meta-go-lockdown`.
- [x] (2026-08-07) Proved no remote branch owns `tidb-meta`, published the
  ownership announcement to both remotes, and refused a second `tidb-stats`
  unit because origin already carries an ownership branch for that crate.
- [x] (2026-08-07) Resolved the ranked `TestMeta` gap to the complete owner
  `pkg/meta/meta.go`: 2,219 lines, 68,013 bytes, SHA-256
  `d0e948b97582b9f1e43ed98f6e3c2664ab71a0051161b5152768c439b0129083`.
- [x] (2026-08-07) Parsed the owner with Go's AST: 154 ordered functions and
  498 outcomes across `if`, loop, logical short-circuit, switch/case, and
  select loci. Identified 35 test/support functions, the `benchCases`
  declaration, and Bazel membership as the complete source-owned support set.
- [x] (2026-08-07) Derived the complete production and support inventory: 774
  rows comprising 85 declarations, 154 functions, 498 branch outcomes, 35
  support functions, one support declaration, and one build-support row. All
  737 production rows are `PORTED`; 13 support rows are source-owned
  `DECLINED` items with quoted ownership or benchmark rationale.
- [x] (2026-08-07) Added the transaction-neutral metadata storage seam and
  ported every
  `meta.go` operation, including the complete `TestMeta` live-mutator flow
  with raw storage failure injection proving source-order partial mutation.
- [x] (2026-08-07) Added source drift, one-verdict, receipt, and compile-symbol
  gates beside the Rust implementation; the gate pins 211 unique production
  Rust symbol families and the ordered inventory identity digest.
- [x] (2026-08-07) Integrated the exact dual-pushed `tidb-model`
  `resource_group.go` lockdown SHA `aac19a3359d973e6ccff0f428d74782af2814703`
  and replaced the provisional generic codec with concrete
  `ResourceGroupInfo`. The meta receipt pins implicit-default pointer identity,
  default settings, exact magic-byte JSON, `RURate` 100 to 200, mixed-case
  `CIStr`, update, reread, list, and drop behavior.
- [x] (2026-08-07) Mutation-probed 19 independent rule families in disposable
  worktrees. One first-pass
  survivor exposed duplicated reverse-iterator boundary logic; the two paths
  were consolidated and the same inclusive-boundary mutation then failed.
- [ ] Run Ready and independent clean-worktree gates at the exact returned
  SHA, verify ratchets directly, dual-push, and reclaim only unit-owned paths.

## Surprises & Discoveries

- Observation: rank 18 is not another key-codec gap.
  Evidence: `rust/crates/tidb-meta/tests/go_vectors.rs` already pins captured Go
  key bytes, while `rust/docs/operations/test-coverage-gaps.md` explicitly
  leaves the live `Mutator` portion of `TestMeta` open.

- Observation: the owner is large and crossed the Rust crate's former stated
  boundary.
  Evidence: `pkg/meta/meta.go` has 154 functions and owns both pure codecs and
  transaction-backed behavior; the crate documentation now names the generic
  `RawTransaction` seam and the in-memory source-parity implementation.

- Observation: `tidb-stats` cannot be used for a second concurrent unit.
  Evidence: origin has
  `codex/task325-tidb-stats-histogram-lockdown` at the prior accepted base,
  which the collaboration contract recognizes as a valid ownership
  announcement; absence on `ngaut` does not authorize a competing owner.

- Observation: `GenGlobalIDs` mutates before its negative-count panic, and a
  signed overflow is not rejected by the later maximum-user-ID check.
  Evidence: a failpoint-safe Go overlay probe observed stored `-1` after
  `GenGlobalIDs(-1)`, `math.MinInt64` after incrementing `math.MaxInt64`, and
  the over-limit value retained after the returned error. The Rust source-order
  path now has the same mutation boundaries.

- Observation: an empty policy value panics in Go's `detachMagicByte`; it does
  not return a malformed-value error.
  Evidence: `value[:1]` and `value[1:]` execute before the magic-type switch.
  The prior Rust helper returned `MalformedKey`; the port now preserves the
  panic and distinguishes incompatible JSON-range bytes from unknown bytes.

- Observation: the transaction owner needs model serialization that the
  accepted-base `tidb-model` seed did not expose for resource groups.
  Evidence: masking-policy and resource-group structs have no serde
  implementation and the complete DDL `Job` type is absent. An exclusive
  `tidb-model` lockdown unit owns `pkg/meta/model/resource_group.go`; this unit
  will consume only its dual-pushed final SHA after independent gating.

## Decision Log

- Decision: keep the storage boundary generic and synchronous inside
  `tidb-meta`, rather than binding metadata rules to a concrete TiKV client.
  Rationale: Go's `Mutator` depends on the behavioral `kv.Transaction` and
  `structure.TxStructure` surfaces. A small Rust trait exposing point and
  ordered-prefix reads plus set/delete operations makes the same metadata rules
  testable in memory and adaptable to the production transaction owner without
  creating a reverse dependency from codecs to the SQL executor.
  Date/Author: 2026-08-07 / Codex.

- Decision: do not close only the `TestMeta` assertions.
  Rationale: the lockdown contract names `meta.go` as the claim boundary. A
  test-shaped subset would leave other functions and branches silent and would
  be divergence-driven work on an already owned source.
  Date/Author: 2026-08-07 / Codex.

- Decision: preserve Go partial mutation, panic, and short-circuit behavior
  even where a safer Rust result would be easier.
  Rationale: negative ID batches, empty magic-byte values, and the mixed
  `&&`/`||` job filter are observable source behavior. Converting them into
  validation errors would not be semantic parity.
  Date/Author: 2026-08-07 / Codex.

## Outcomes & Retrospective

The exact 154-function/498-outcome production denominator and complete support
denominator are checked in. The implementation and inventory gates pass the
crate's complete default-feature test surface, including captured Go Job
vectors and source-order storage-failure boundaries. Mutation proof, concrete
resource-group integration, Ready validation, the clean-workspace gate, and
dual-remote shipping remain; no final completion or ratchet claim is made at
this checkpoint.

## Context and Orientation

`pkg/meta/meta.go` is the authoritative Go production source. It builds
metadata operations over `structure.TxStructure`, which maps logical string
and hash operations into raw keys under the single-byte `m` prefix. The
original tests are primarily in `pkg/meta/meta_test.go`; `pkg/meta/main_test.go`
supplies package test setup and `pkg/meta/BUILD.bazel` records build membership.
There is no package `doc.go`.

Rust's current `rust/crates/tidb-meta/src/key.rs`, `structure.rs`, `value.rs`,
and `element.rs` implement deterministic portions of the same source.
`rust/crates/tidb-meta/tests/go_vectors.rs` and
`key_prefix_and_element_source.rs` are existing receipts, not a complete
transactional port. A metadata transaction seam means an interface whose
implementation supplies raw point reads, ordered range scans, writes, and
deletes while `tidb-meta` alone owns how logical metadata operations translate
to those raw actions.

## Plan of Work

First, use Go's parser over the exact source to emit the ordered function and
control-locus identities. Read every original test and support declaration and
attribute it to the owning functions. Pin hashes for the source, test setup,
tests, and Bazel membership. Direct Go probes must cover absent values,
malformed integer/JSON/magic bytes, overflow and domain limits, duplicate
create/update/drop behavior, ordered iteration, short-circuit paths, and
partial mutation when a later operation fails.

Second, add `rust/crates/tidb-meta/src/transaction.rs`. Define a minimal raw
transaction trait and a deterministic in-memory implementation used only as a
test oracle. Implement the string and hash operations that Go obtains from
`TxStructure`: missing reads, decimal integer increments with overflow, ordered
iteration, prefix clearing, and exact encoded keys. The metadata `Mutator`
then owns global/schema ID allocation, database/table lifecycle, auto-ID and
sequence fields, bootstrap/version flags, policy/resource state, DDL history,
schema diffs, and remaining helpers in source order.

Third, extend existing `key`, `value`, and `element` modules only for behavior
actually owned by `meta.go`; do not duplicate codecs. Model-facing methods use
the existing `tidb-model` representations when available. If an exact source
type is absent, add only the minimum source-owned representation after proving
that no other active crate owns that Rust module; record any expanded crate
reservation before editing it.

Fourth, add `meta_go_inventory.rs` beside the implementation and register it
under tests. Every Go function and both outcomes of every decision must have
one of `PORTED`, `DECLINED`, or `UNREACHABLE`, with concrete Rust symbols or
source/probe proof. The gate must reject duplicates, missing rows, empty
evidence, placeholder text, source drift, and vanished symbols.

Finally, mutation-probe each independent rule family from an immutable
provisional commit in a disposable worktree. Run the crate's complete tests,
warnings-denied clippy, formatting, `make -j12 lint`, and the full workspace in
a clean detached worktree with an exclusive target directory. Push one exact
SHA to both remotes only after direct inventory and ratchet greps agree.

## Concrete Steps

All commands run in the isolated worktree, never the divergent main checkout.
Cargo commands run serially with the unit-exclusive target directory.

    cd rust
    CARGO_BUILD_JOBS=12 \
      CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-tidb-meta \
      cargo test --locked -j12 -p tidb-meta --all-targets

Targeted Go tests use the failpoint decision documented by
`.agents/skills/tidb-failpoint-test-runner/SKILL.md`; the exact wrapper command
will be recorded after checking package failpoint imports.

Ready validation additionally includes:

    cd rust
    CARGO_BUILD_JOBS=12 \
      CARGO_TARGET_DIR=/private/tmp/cargo-target-task325-tidb-meta \
      cargo clippy --locked -j12 -p tidb-meta --all-targets -- -D warnings
    cargo fmt --all -- --check
    cd ..
    git diff --check
    make -j12 lint

## Validation and Acceptance

Acceptance requires the complete Go source inventory to pass, not merely the
ranked test name. The Rust equivalent of `TestMeta` must demonstrate concurrent
non-overlapping global-ID batches; absent schema version zero; database and
table duplicate/not-found errors; table revision and ordered iteration;
allocator cleanup; bootstrap and schema-diff round trips; BDR role lifecycle;
and exact DDL history key bytes. Every other source function and branch must
have an equally concrete receipt or a source-backed `DECLINED`/`UNREACHABLE`
proof. Every deliberate boundary mutation must make its intended test fail.

The final clean-worktree gate must exercise the full workspace at the returned
SHA. Any pre-existing aggregate failure is recorded literally and isolated by
fresh-process partitions; it is never reported as a passing aggregate command.

## Mutation Receipt

Each row changed the implementation rule, not an expected answer. `KILLED`
means the named boundary test or lockdown gate returned nonzero. The inclusive
history mutation initially survived because identical predicates existed in
the trait default and `MemoryTransaction`; consolidating them into
`owned_reverse_iterator` removed that blind path, and the repeated mutation
was killed by the same source-semantic test.

| Rule family | Boundary mutation | Intended receipt | Result |
| --- | --- | --- | --- |
| magic-byte version | `0x00` to `0x01` | `magic_byte_matches_go` | KILLED |
| table revision | increment by 1 to increment by 0 | `database_and_table_lifecycle_preserves_go_order_and_partial_mutation` | KILLED |
| global-ID limit | `>` to `>=` | `global_id_zero_limit_error_and_signed_wrap_match_go_mutation_order` | KILLED |
| partial write order | allocator write before table create | `raw_storage_failures_propagate_and_keep_go_partial_mutation_order` | KILLED |
| must-load fallback | missing foreign-key marker true to false | `source_range_partial_json_and_filter_boundaries` | KILLED |
| job-filter precedence | Go mixed precedence to symmetric conjunction | `job_name_filter_keeps_go_operator_precedence` | KILLED |
| metadata lock | exact `"1"` to any non-`"0"` | `malformed_scalar_storage_returns_the_source_parse_error_class` | KILLED |
| history start boundary | inclusive `<=` to exclusive `<` | `ddl_history_is_big_endian_reverse_inclusive_and_filtered_before_decode` | SURVIVED, CONSOLIDATED, KILLED |
| source drift | owner line count 2,219 to 2,218 | `source_inputs_match` | KILLED |
| symbol drift | rename `schema_cache_size` | `every_ported_production_symbol_is_compile_anchored` | KILLED |
| kernel branch | invert `is_next_gen` | next-gen `system_database_creation_and_iteration_cover_classic_and_nextgen_rules` | KILLED |
| element length | exact length accepted to rejected | `element_round_trips_and_reports_gos_two_failures` | KILLED |
| empty magic value | source panic to returned error | `policy_masking_and_resource_reads_preserve_magic_json_and_empty_panics` | KILLED |
| scalar float | fixed two decimals to three | `scalar_settings_preserve_absence_formatting_and_non_boolean_lock_bytes` | KILLED |
| snapshot timestamp | `start_ts` to `start_ts + 1` | `iter_all_tables_clamps_workers_streams_ranges_and_serializes_callbacks` | KILLED |
| inventory verdict | production `PORTED` to `DECLINED` | `inventory_is_complete_unique_and_classified` | KILLED |
| concrete resource add | suppress persisted model JSON | `policies_masking_policies_and_resource_groups_preserve_source_lifecycle` | KILLED |
| concrete resource update | suppress `RURate` 100 to 200 write | `policies_masking_policies_and_resource_groups_preserve_source_lifecycle` | KILLED |
| implicit default identity | allocate a fresh `Arc` per read | `policies_masking_policies_and_resource_groups_preserve_source_lifecycle` | KILLED |

The consumed `resource_group.go` lockdown independently executed 66 mutation
attempts across all model-owned rules. One first-pass nested-`CIStr` survivor
caused a lowercase-only vector to be added; all 65 final mutants were killed.
Its query, catalog, table, and integration ratchets remained exactly
`0`, `100`, `1`, and `75`.

## Idempotence and Recovery

The ownership branch and all test commands are safe to rerun. Each mutation
uses a disposable detached worktree and must leave it clean before the next
mutation. Never force-push. Before deleting a worktree or target directory,
verify the final SHA is present on both remotes and that the exact path belongs
to this unit.

## Artifacts and Notes

Accepted base and ownership refs:

    b5c1aee5678778f68947fcfbfae4901464fe58da
    refs/heads/codex/task325-tidb-meta-meta-go-lockdown

Pinned source inputs at the accepted base:

    pkg/meta/meta.go        d0e948b97582b9f1e43ed98f6e3c2664ab71a0051161b5152768c439b0129083
    pkg/meta/meta_test.go   c306fd0d4af006551eded4552e707951780bdedf99cd03d6fc134cc0233a39da
    pkg/meta/main_test.go   3140c9451d6bac5c74455f15cbe67530e3e2cb052ceacd0a762002e524d307e8
    pkg/meta/BUILD.bazel    db0b61627145acc6abbcf0e084396d34abe1fc256a357351ce9c9d73944b1c9d

## Interfaces and Dependencies

`rust/crates/tidb-meta/src/transaction.rs` defines a transaction trait
whose operations are expressed in raw encoded keys and values. It must support
point get, ordered half-open range scan, set, and delete. `Mutator<T>` owns a
transaction value or mutable borrow and exposes source-shaped metadata
methods. Missing raw keys are `None`; source methods translate that to Go's
zero, nil, or not-found behavior as appropriate.

`tidb-meta` continues depending on `tidb-codec`, `tidb-model`, `serde`, and
`serde_json`. A dependency on a concrete TiKV transport or on `tidb-exec` is
forbidden because it would invert the existing dependency direction. A
production adapter belongs with the transaction owner after the source module
is complete.

Revision note (2026-08-07): updated after implementation, inventory, source
probes, and default-feature crate gates; mutation, Ready, clean-workspace, and
shipping receipts remain open.
