# Lock down `pkg/meta/model/resource_group.go`

This lockdown owns one complete Go source and its Rust landing module:

- Go authority: `pkg/meta/model/resource_group.go`
- Rust implementation: `rust/crates/tidb-model/src/resource_group.rs`
- Inventory/gate: `rust/crates/tidb-model/src/resource_group_go_inventory.rs`
- Boundary tests: `rust/crates/tidb-model/tests/resource_group_lockdown.rs`

The pinned Go owner is 6,612 bytes and 191 lines with SHA-256
`f1275f07a14c09e4b5c608f896c6620e5c8525b4bbd2b4f51e94e076208bae47`.
The inventory classifies all 26 ordered named declarations, all six functions,
and all 41 outcomes of the 19 `if` statements and one three-way switch. Every
row is PORTED. There are no DECLINED or UNREACHABLE rows, and there is no
adjacent `pkg/meta/model/resource_group_test.go` test/support declaration.

This lockdown moves no differential oracle or ratchet. That is a success: the
deliverable is complete source ownership and executable drift protection, not
ratchet movement.

## Measured Go contract

A direct program against the pinned Go source established these boundaries:

- a zero `ResourceGroupSettings` emits all eight fields; the constructor differs
  only by setting priority to medium (`8`);
- a nil anonymous `*ResourceGroupSettings` omits all embedded settings fields,
  while any matching settings key, even a JSON `null`, allocates it on decode;
- nil and allocated-empty `JobTypes` slices encode as `null` and `[]`
  respectively;
- every action/watch `int32`, state byte, signed integer, and unsigned integer
  boundary survives JSON, while out-of-range JSON numbers fail;
- decoding is ASCII case-insensitive, later duplicate values win, a `null` is a
  no-op for non-pointer scalar/struct fields, and a `null` clears pointer/slice
  fields;
- `ast.CIStr` preserves independently supplied `O` and `L` values, including
  case-insensitive and duplicate nested keys;
- embedded field order, HTML/U+2028 escaping, and full-width numeric formatting
  match Go's `encoding/json` bytes;
- `String` preserves every separator, quote, unknown action/watch fallback,
  nil nested block, and wrapping `time.Duration` conversion, including
  `math.MaxUint64` milliseconds rendering as `-1ms`;
- `Adjust` preserves the unlimited sentinel and negative burst modes, and uses
  Go's wrapping `uint64` to `int64` conversion otherwise;
- `ResourceGroupSettings.Clone` allocates a new top-level settings value but
  shares the runaway/background pointers and their job slice; `ResourceGroupInfo.Clone`
  allocates a new settings value with the same nested aliases and panics when
  its embedded settings pointer is nil.

The Rust persistence boundary therefore uses open signed-32-bit wrappers for
action/watch ordinals and a concurrency-safe shared pointer wrapper for the two
nested Go pointers. This avoids silently folding unknown future ordinals or
turning Go's shallow copy into a deep copy.

## Gates

The inventory test rejects source hash, byte-count, line-count, ordered function,
cardinality, duplicate-row, empty-receipt, serde, and PORTED-symbol drift. The
boundary tests pin zero/full JSON, embedded nil allocation, case/duplicate/null
rules, `CIStr`, nil versus empty slices, integer widths, all string branches,
adjustment boundaries, and shallow pointer aliasing.

Scoped validation uses a worktree-exclusive target directory:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo test --locked -j12 -p tidb-model --all-targets

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo clippy --locked -j12 -p tidb-model --all-targets -- -D warnings

## Mutation proof

Mutation probes ran only in disposable detached worktrees at immutable
provisional commits `ed5a371c670a1e95380abcda3291676c2716b4ca` and
`a2d76b68b8ad9bd400b25689835bb1fddd604a7c`; the authoritative worktree was
never mutated. All 65 independent mutants were killed in 66 attempts:

- 1 source-byte/hash mutant by
  `resource_group_go_inventory::resource_group_go_source_identity_is_current`;
- 1 disappeared PORTED-symbol mutant by
  `resource_group_go_inventory::every_ported_resource_group_symbol_still_compiles`;
- 4 constructor, unlimited-sentinel, nonnegative-adjustment, and wrapping-cast
  mutants by `resource_group::tests::constructors_and_adjustment_boundaries`;
- 4 top-level-copy, nested runaway/background alias, and nil-settings panic
  mutants by
  `resource_group::tests::clone_is_top_level_copy_with_shared_nested_pointers`;
- 23 duration-overflow and every `String` branch/separator/fallback mutants by
  `resource_group_lockdown::settings_string_pins_every_source_branch_boundary`;
- 8 standalone settings field-name/order/default mutants by
  `resource_group_lockdown::zero_settings_json_matches_go`;
- 15 embedded/full-width/nested-field/order mutants by
  `resource_group_lockdown::info_nil_embedding_and_full_json_match_go`;
- 7 allocation/case/duplicate/null/pointer/slice/`CIStr` mutants by
  `resource_group_lockdown::decode_matches_go_embedding_null_case_duplicate_and_ci_string_rules`;
- 1 state-byte-width mutant by
  `resource_group_lockdown::numeric_json_rejects_values_outside_go_underlying_widths`;
- 1 background-utilization field-name mutant by
  `resource_group_lockdown::nil_and_allocated_empty_job_slices_remain_distinct`.

The first nested-`CIStr` case-folding attempt survived because a later uppercase
duplicate masked the missing lowercase read. That successful falsification led
to a lowercase-only boundary vector; the identical mutant then failed. No
mutant survived the strengthened suite.
