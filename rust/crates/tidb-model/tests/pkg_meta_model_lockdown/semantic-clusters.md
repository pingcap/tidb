# `pkg/meta/model` package lockdown semantic clusters

This is one atomic package claim over the fourteen production Go files, eight
direct Go test files, and `BUILD.bazel`. It is not a collection of independent
file-completion claims. Go at source seed
`bdab0016365e8b1d79b5b11f52ee6fdde90f4c46` is the source of truth.

The package AST census contains 4,236 obligations: 2,481 production and 1,755
test/support obligations. The existing checked-in locks for `job_args.go` plus
`job_args_test.go` (1,612 obligations), `masking_policy.go` (39), and
`resource_group.go` (76) are absorbed unchanged. They are not reopened. The
remaining package census is 2,509 obligations plus the build artifact.

## C01: BDR, database, flags, and table-mode leaves

- Go: `bdr.go`, `bdr_test.go`, `db.go`, `flags.go`, `table_mode.go`,
  `table_mode_test.go`.
- Rust: `bdr.rs`, `db.rs`, `flags.rs`, `table_mode.rs`.
- Rules: action/role maps, TSO physical-time extraction, database ordering,
  numeric flags, and the complete table-mode transition matrix.
- Explicit representation boundary: Go `DBInfo.Copy` shares table pointers;
  Rust's owned `Vec<TableInfo>` cannot expose that alias identity. Value,
  order, and deep clone behavior remain testable. Go's `time.Time` range is
  wider than chrono's; out-of-range physical milliseconds are classified at
  the representation boundary rather than silently called equivalent.

## C02: columns and engine attributes

- Go: `column.go`, `column_test.go`, `engine_attribute.go`.
- Rust: `column.rs`, `engine_attribute.rs`.
- Rules: changing/removing names, field-type delegation, generated columns,
  BIT defaults, lookup/extra-column constructors, engine JSON, storage-class
  scope, transition rendering, wrapping unsigned duration arithmetic, omitted
  scalar zero values, the recursive Go `any` JSON domain (null, booleans,
  strings, arrays, and sorted-key objects), numbers decoding to nearest
  float64 (including integer lexical forms and precision loss beyond 2^53),
  non-finite float serialization errors, and nil elements in
  `StorageClassSettings.Defs`' owning Go pointer slice.
  Name normalization uses Go's Unicode simple-rune case tables; it never
  introduces Rust full-case multi-rune expansions.
- Explicit representation boundaries: nil versus allocated-empty dependency
  maps, shallow clone pointer identity, Go
  `unsafe.Sizeof`, and byte-exact `json.RawMessage` lexical preservation.
  Parsed JSON meaning remains native; lexical whitespace/key-order/duplicate
  preservation is not falsely reported as ported.

## C03: indexes

- Go: `index.go`, `index_test.go`.
- Rust: `index.rs` plus `table_info.rs` compile anchors.
- Rules: global-index versions/flag, changing names, inverted physical types,
  exact bidirectional indexable-function/distance-metric maps, full-text names,
  prefix detection, table-column ID resolution, columnar type,
  partial-condition parsing, ordinary and foreign-key prefix coverage, ID/name
  lookup, Go simple-rune lower/upper case normalization (including parser
  keyword lookup without full-case ligature expansion), and unknown persisted
  enum round trips.
- Explicit representation boundaries: Go's mixed shallow/deep clone pointer
  aliases, nil elements in pointer slices, nil versus allocated-empty index
  column/region-split slices in the existing `Vec` representation, and the TiDB
  process-wide `kerneltype.IsNextGen()` startup hook. The mutable flag and both
  values are native; callers must select the Rust startup value explicitly.
  Rust's typed equality cannot receive Go's arbitrary `any` or a typed nil
  pointer; non-nil `IndexInfo` identity and hashing are exactly ID-only.

## C04: placement

- Go: `placement.go`, `placement_test.go`.
- Rust: `placement.rs`, `setting_builder.rs`.
- Rules: stable rendering order, string escaping, non-zero integer emission,
  duration/item formatting, deep settings clone, and the source nil-pointer
  invariant for policy clone.
- Explicit representation boundary: package-private Go helpers accept an
  arbitrary variadic list of side-effecting separator closures. All owning Go
  call sites use the represented default or one explicit separator string;
  arbitrary callback identity/effects are not exposed as a public Rust API.

## C05: reorganization metadata

- Go: `reorg.go`; direct support in `job_test.go`.
- Rust: `reorg.rs` and `job.rs` timezone/job-meta leaves.
- Rules: states/stages/types, process-default fallback for old zero metadata,
  mutable concurrency/batch/max-speed, captured collation fallback, backfill
  JSON, byte-slice base64, job metadata, and first-resolution-stable cached
  fixed/named timezones. Go's 64-bit `int` range is retained for persisted
  maximum-node counts, dynamic settings, job priority, and fixed-zone offsets.
- Explicit representation boundaries: Go error pointer/stack identity,
  atomic/mutex object identity, and the warning-map backing-store alias retained
  by `DDLReorgMeta.ShallowCopy`. `BackfillMeta.Decode` uses the package's
  receiver-mutating Go object-stream decoder: omitted fields survive; duplicate
  and Go Unicode-SimpleFold tags apply in source order; null scalars are no-ops;
  null pointers/slices/maps clear; existing maps merge; and mutations before
  and after recoverable type, overflow, fractional-number, or base64 errors
  survive while the first error is returned. Invalid syntax is rejected before
  mutation and root JSON null is a no-op. Atomic metadata fields reset to
  local zero on null and their custom-unmarshal failures abort immediately.
  Primitive null map values insert their zero without error. Error values use the native
  `tidb-error` class/code/message/RFC JSON envelope (including nullable warning
  map entries), so scalar/array impostors are fatal custom-unmarshal failures:
  they are neither inserted nor followed by later member mutation. Rust
  borrowing makes field mutation race-free.

## C06: DDL jobs

- Go: `job.go`, `job_test.go`.
- Rust: `action_type.rs`, `job_enums.rs`, `job.rs`, `schema_diff.rs`,
  `schema_state.rs`, with `job_args.rs` absorbed unchanged.
- Rules: all enums/names, the reserved 200..256 action-ID range, classic v1 initialization, lifecycle predicates,
  finish/history updates, row/warning access, v1/v2 raw-argument envelope,
  pause/resume causes, pausable/alterable/resumable rules, reorg detection,
  rollback matrix, system variables, scheduler involvement normalization and
  validation, sub-job proxy state, and multi-schema revertibility. Scheduler
  involvement names use Go's Unicode simple-rune lowercasing rather than
  Rust's full-case expansion, while preserving the `*` and empty sentinels.
  `SubJob.Clone` clears its private decoded-argument cache while retaining the
  raw persisted envelope; `Job.Clone` refreshes the source job's raw argument
  envelope before decoding its copy, preserving the source-visible side
  effect. Persisted `json.RawMessage` arguments retain their validated owned
  JSON text exactly on decode, including duplicate members, member order,
  numeric lexical forms, and insignificant whitespace. Re-encoding follows
  Go `json.Marshal`: it compacts only insignificant whitespace and applies the
  parent encoder's HTML escaping without parsing the raw envelope through a
  lossy generic value. The decoded `Args` cache remains separate. `JobW`
  retains arbitrary original bytes without
  decoding or normalizing them and dereferences to its embedded job. Rust's
  owned wrapper cannot represent Go's nil embedded `*Job` or byte-slice alias
  identity. The decoded-argument cache preserves Go's nil
  versus allocated-empty distinction because v1 marshals those as `null`
  versus `[]`, and a nil sub-job cache suppresses raw-argument replacement.
  The same distinction is retained for `MultiSchemaInfo.SubJobs`,
  and `HistoryInfo.MultipleTableInfos` because those cases alter JSON; the
  pointer slices retain null elements and recursively reuse existing pointees.
  Runtime
  scheduler involvement follows Go's `len > 0` rule: both nil and allocated
  empty select the schema/table fallback, while a nonempty slice is explicit.
  All non-serialized
  `MultiSchemaInfo` bookkeeping fields remain native Rust fields. V2 argument
  encoding enforces the source's test-build one-value invariant for both the
  parent job and executing sub-jobs. `ToProxyJob` accepts the source 64-bit
  `int` range and performs the same narrowing conversion into persisted
  `MultiSchemaInfo.Seq`; job and backfill priorities retain the full range.
- Explicit representation boundaries: Go error pointer/stack identity, Go
  mutex identity, the Go-only `unsafe.Sizeof(Job/SubJob)` ABI guard,
  nil elements inside Go pointer slices other than history tables/sub-jobs, and
  arbitrary typed `JobArgs` implementations. The same missing process-wide
  NextGen boundary means Rust initializes the new-job version to classic v1;
  explicit `set_job_ver_in_use` and both persisted versions are native. As for
  backfill metadata, a dedicated object-stream decoder preserves omitted
  receiver fields, processes duplicate and Unicode-SimpleFold tags in source
  order, ignores unknown fields, treats null nonpointer scalars as no-ops,
  clears null pointer/slice/map fields, merges existing maps, returns the first
  recoverable field error, and keeps consuming later keys so their mutations
  survive. A syntax-validation pass precedes receiver mutation, and root JSON
  null is a no-op, matching Go's scanner and `json.Unmarshal` receiver
  behavior. `terror.Error` fields use `tidb-error`'s compatible typed envelope;
  an error from their custom JSON decoder is fatal at every nesting level,
  while ordinary type/base64/overflow errors retain first-error continuation.
  `TraceInfo` natively validates its string, base64 byte slice, and `uint64`
  fields. Existing `HistoryInfo.DBInfo` and `TableInfo` allocations are merged
  recursively: omitted fields survive, null pointers clear, null scalars are
  no-ops, and a recoverable nested field or table-list element error does not
  suppress later nested, sibling, or outer job mutations. Nested reorganization,
  timezone, job-meta, pause/resume, multi-schema, and history envelopes use the
  same receiver-mutating object path instead of Serde's default-resetting
  generated `deserialize_in_place` implementation.
  Typed job arguments remain owned by the unchanged `job_args.go` lockdown;
  the job envelope stores their JSON value rather than pretending every Go
  interface implementation is a Rust type.

## C07: tables and partitions

- Go: `table.go`, `table_test.go`.
- Rust: `table.rs`, `table_info.rs`, `partition.rs`.
- Rules: table identity, PK/auto-ID lookup, public offset slots including nil
  gaps, index/constraint/FK lookup, column movement and dependent offsets,
  placement clearing, storage class, table kinds/locks, partition state/GC,
  default LIST partition, overlapping DROP replacement, DDL-hidden IDs,
  Go simple-rune name lookup, foreign-key rendering, statistics keys/defaults, TTL duration compatibility,
  affinity normalization, and statistics-window RFC3339 values including Go's
  year-1 zero time and non-UTC fixed offsets. A non-nil partition definition's
  memory estimate uses the owning Go 64-bit object/header sizes and byte-length
  payload accounting, independent of Rust's object layout.
- Explicit representation boundaries: pointer/slice alias
  identity and nil elements in pointer slices, allocation identity for the
  existing Rust `Vec`-backed partition metadata fields (including the direct
  `DDLColumns == nil` support assertion), map iteration order (semantically
  unspecified by Go), and the test-only TTL failpoint override. The normal
  duration rule is native; every JSON-relevant nil/empty boundary with an
  explicit `Option` representation is ported and mutation-pinned. Rust's typed
  equality cannot receive Go's arbitrary `any` or typed nil pointer; non-nil
  `TableInfo` identity and hashing are exactly ID-only.
  The nil-receiver branch of `PartitionDefinition.MemoryUsage` remains a
  pointer-representation boundary: safe Rust cannot invoke an `&self` method on
  a null reference; an optional caller obtains the same zero with `map_or`.
  Persisted column/index offsets and index prefix lengths use the pre-existing
  Rust `i32` representation rather than Go's 64-bit `int`. Values outside
  `i32` are not representable; all schema-valid offsets/lengths, including
  negative and upper-bound panic behavior exercised by the owning source, stay
  native. This measured width boundary must be classified explicitly in the
  generated ledgers rather than hidden behind a whole-struct verdict.

## C08: absorbed source locks and package build contract

- Go/Rust inventory locks absorbed without rewriting their historical
  receipts: `job_args.go` + `job_args_test.go` / `job_args.rs`,
  `masking_policy.go` / `masking_policy.rs`, and `resource_group.go` /
  `resource_group.rs`. The package decoder extension routes the latter two
  modules' existing custom persisted-field comparisons through the shared Go
  Unicode-SimpleFold matcher; long-s and Kelvin-sign boundaries pin the rule.
- Build owner: `pkg/meta/model/BUILD.bazel`; the package receipt pins all 14
  library sources, all 8 tests, `embedsrcs = ["job.go"]`, `flaky = True`,
  `shard_count = 50`, timeout, and dependency lists.
- Zero artifact classes: no build tags, platform variants, generated files,
  `go:generate`, `go:embed`, or tracked testdata in the root package.

No cluster is complete merely because its Rust module exists. The generated
obligation ledger, symbol gate, source hashes, mutation results, scoped Ready
gate, and coordinator full-workspace gate together determine completion.

## Measured existing-architecture slice boundary

The owning Go structs use slices, so a nil slice and an allocated empty slice
can encode as `null` and `[]` when the field lacks `omitempty`. The pre-existing
Rust model uses `Vec` for the following fields and its `null_if_empty` wire
adapter intentionally maps both empty states to `null`: `IndexInfo.Columns`,
`IndexInfo.AffectColumn`, `RegionSplitPolicy.Lower/Upper`,
`TableInfo.Columns/Indices/Constraints/ForeignKeys`,
`TableInfo.StorageClassTransitions`,
`PartitionDefinition.LessThan/InValues`, `PartitionInfo.Columns/Definitions/
AddingDefinitions/DroppingDefinitions/NewPartitionIDs/
OriginalPartitionIDsOrder/States/DDLColumns/DDLUpdateIndexes`,
`PartitionDefinition.StorageClassTransitions`, `ViewInfo.Cols`,
`ConstraintInfo.ConstraintCols`, `FKInfo.RefCols/Cols`, `ReferredFKInfo.Cols`,
`TableLockInfo.Sessions`, and `TiFlashReplicaInfo.LocationLabels/
AvailablePartitionIDs`. The same allocation-identity limitation applies to
the runtime-only `DBInfo.Deprecated.Tables`, `MultiSchemaInfo.AddColumns/
DropColumns/ModifyColumns/AddIndexes/DropIndexes/AlterIndexes/AddForeignKeys/
RelativeColumns/PositionColumns`, and `AddForeignKeyInfo.Cols`, plus the
pre-existing `DBInfo.TableName2ID` and `PartitionInfo.DDLChangedIndex` map
representations. These exact allocation-identity/wire branches must be
DECLINED individually in the generated ledgers with this measured Rust boundary;
ordinary element values, ordering, empty-length behavior, and all methods that
do not observe allocation identity remain eligible for PORTED verdicts. Fields
changed in this unit to `Option<Vec<_>>` are not part of this decline.
