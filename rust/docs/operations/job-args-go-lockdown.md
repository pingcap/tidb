# `job_args.go` source lockdown receipt

This is a file seed, not a claim that the complete Go `pkg/meta/model` package
has been transcreated. Go `pkg/meta/model/job_args.go` is authoritative.
Existing lockdowns for `masking_policy.go` and `resource_group.go` are unchanged.

## Pinned inputs and census

| Input | SHA-256 | Bytes | Lines |
| --- | --- | ---: | ---: |
| `pkg/meta/model/job_args.go` | `da1b59226cb2d3a05bbb65c945709088a33fac605237b4e195377c10ebec27bf` | 65,331 | 1,917 |
| `pkg/meta/model/job_args_test.go` | `7647a990a36ff19f4bc1e56ed915466c6be7c87c32bd0c68a443e24378edb382` | 39,148 | 1,242 |

The isolated `go_package_lockdown_inventory` census contains 1,612 unique
obligations. Production contributes 637: 161 functions, 186 branch outcomes,
24 loops, 18 short-circuit outcomes, 13 switch cases, 55 declarations, 177
fields, and 3 constants. Direct test/support contributes 975: 44 tests, 240
assertions, 32 branches, 2 helpers, 4 helper closures, 144 loops, and 509 rows.

The checked-in ledger records exactly one verdict per obligation: 24 `PORTED`,
1,588 `DECLINED`, and zero `UNREACHABLE`. The ported surface is the Go iota and
byte width of `IndexOp`, the complete `RenameTableArgs` data shape, the three
outcomes of `IndexArg.GetColumnarIndexType`, and the parallel-slice semantics of
`GetRenameTablesArgsFromV1`.

The declined boundary is measured, not speculative. Every one of the 44 Go
tests constructs a `Job` through `getJobBytes` or `getFinishedJobBytes`, then
uses `FillArgs`/`FillFinishedArgs`, `Encode`, `Decode`, or `RawArgs`. The accepted
Rust `job.rs` explicitly defers "the `Job` struct itself" and the
"version-dependent JSON args (`RawArgs`/`Encode`/`Decode`/`FillArgs`)". Porting
those rows here would fabricate a second `Job` owner and reopen a different Go
source. The ledger gate pins both sides of this measured boundary.

## Boundary mutation receipts

Each mutation changed an implementation rule while keeping the expected answer
unchanged. All commands returned nonzero at the named test. The first version
of the parallel-slice test let a missing first slice fall through to a later
panic; the survivor caused the test to check each of the five parallel slices
independently, after which the mutation was killed.

| Rule | Mutation | Receipt | Result |
| --- | --- | --- | --- |
| legacy columnar fallback | `NA && is_columnar` to `NA && !is_columnar` | `columnar_index_type_preserves_all_source_boundaries` | KILLED |
| iota values | rollback value `2` to `1` | `index_operation_values_keep_go_iota_and_byte_width` | KILLED |
| parallel order | `new_schema_ids[index]` to `[0]` | `rename_parallel_slices_keep_source_order_and_json_boundaries` | KILLED |
| runtime-only JSON | `serde(skip)` to persisted default field | `rename_parallel_slices_keep_source_order_and_json_boundaries` | KILLED |
| parallel length panic | first unchecked index to default fallback | `rename_empty_and_mismatched_parallel_slices_match_go` | SURVIVED, TEST STRENGTHENED, KILLED |

Completeness here is the exhaustive classification and proof of the reachable
surface. No result-oracle ratchet movement is expected; unchanged ratchets are
a successful lockdown result.
