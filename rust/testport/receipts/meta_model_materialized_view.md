# `pkg/meta/model` — materialized-view metadata drift receipt (Go-master parity)

Comparison source: Go `origin/master` at commit
`94a9cbedabbb3190fd892a196dd446df48b7ec6e` (2026-09-03). Batch scope: the
package drift from the prior area pin `1c1a334d2b` to `origin/master`
(`git diff --stat` 7 files, +332/−51); the prior full-package audits remain
`b116`/`meta_model_job_args.md` and `tests_pkg_meta_model_part2.rs`.

## Complete Go inventory

The package contains exactly 23 tracked artifacts and 10,721 lines at the
comparison commit. Every production file, test, and Bazel target was read
from `git show origin/master:...` before editing. The package has no
`doc.go`, fixture/testdata directory, generated Go source, benchmark, or
`//go:build` platform variants.

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `BUILD.bazel` | 74 | `da1a8f8cfa25779ecdb57c0e731ce80b30fb534f` |
| `bdr.go` | 144 | `b017482deed562c94ed6939c36b4faeba2a8453d` |
| `bdr_test.go` | 35 | `c247424fa12775f890b6da1abae6f88a133d7c83` |
| `column.go` | 392 | `1ff05e4c967deb5e83fd979e9e8039acf1d73b78` |
| `column_test.go` | 106 | `bd46ccbf01cf5ef66cce7748ea8e1b222b0c0b69` |
| `db.go` | 58 | `ee550b02dd3113a3fc625f5d3e9d8dd36dbba6f4` |
| `engine_attribute.go` | 93 | `c9f11f229b0467d0ac86c2b8a49593cc3cdbea46` |
| `flags.go` | 50 | `233a6a554b683db26a0a2ded3bb7f90a36448588` |
| `index.go` | 579 | `0d22a9806e02ba3c57556a40d353a4e308f4008e` |
| `index_test.go` | 105 | `b2b72782d3dd31be8fe52f83399733047d92adef` |
| `job.go` | 1488 | `7c55483a961c9ca3c9df393c871a880e76036e94` |
| `job_args.go` | 1960 | `3035257cfb9a19c61ae5f7b0ae43186d0bb5bae7` |
| `job_args_test.go` | 1317 | `4a43eb141aea72e88d3085efde713854b029c394` |
| `job_test.go` | 526 | `f017ae9d553c66b3310a707b6844b2e03d38382b` |
| `masking_policy.go` | 93 | `806e4112aed3e1ba77971eb4b48dbc9f13f82442` |
| `placement.go` | 143 | `c6458777ba668dce233a920591def8a0dd8c4ba5` |
| `placement_test.go` | 87 | `5e2b91bed51705cdd337d0ede726bbb6b685ca20` |
| `reorg.go` | 266 | `2f6b59077fe7c9584796bd543c814b30b3f7caeb` |
| `resource_group.go` | 191 | `d7d741631a7ec0c1ff128bf5804ea6bdcb7051f9` |
| `table.go` | 1693 | `92618b12bd9d9b203cc6633cbb8457de19770986` |
| `table_mode.go` | 89 | `8d8525a88146a3524180632157f370c9beb7297b` |
| `table_mode_test.go` | 46 | `1ad0ef5aa8cdeabba58583a9009860a415d54b8e` |
| `table_test.go` | 271 | `c906b4f38c944938055cc12d01a90b798c9c461e` |

## Drift content and Rust alignment

Go master introduced materialized-view DDL metadata (commit `94a9cbedab`).
The Rust owner `tidb-model` had no materialized-view model surface. This
batch implements the missing Go behavior and aligns each drifted hunk:

- `job.go`: `ActionCreateMaterializedViewLog = 85` / `ActionCreateMaterializedView = 86`
  plus `ActionMap` names → `action_type.rs` constants, names, and the
  86-entry declared-set contract; `MayNeedReorg` includes
  `ActionCreateMaterializedView` → `job.rs::may_need_reorg`;
  `IsRollbackable` case (`StateNone`/`StateWriteReorganization` only) →
  `job.rs::is_rollbackable`; `SubJob.InvolvingSchemaInfo`
  (`json:"involving_schema_info,omitempty"`) with `ToProxyJob`/`FromProxyJob`
  propagation → `SubJob` field, serde `skip_serializing_if` emptiness,
  `job_json.rs` merge branch, and both propagation methods;
  `MultiSchemaInfo.InvolvingSchemaInfo` (`json:"-"`, runtime-only) →
  `#[serde(skip)]` field; `TimeZoneLocation.Clone` (nil-safe, cache excluded)
  → covered by the crate's derived `Clone` (the `OnceLock` cache clone
  re-resolves like Go's fresh mutex), pinned by the timezone clone regression.
- `table.go`: `TableInfo.MaterializedViewBase/MaterializedView/MaterializedViewLog`
  (`omitempty` pointers, `Clone` deep-copies) → `table_info.rs` fields with
  `Option::is_none` skips, `clone_like_go` deep clones, and `job_json.rs`
  merge branches; `MaterializedViewBaseInfo`, `MViewInitBuildState`
  (`IsReady`/`String`/`AccessErrorMessage`), `MaterializedViewInfo`
  (`GetInitBuildState` nil→ready, `Clone`), `MaterializedViewLogInfo`
  (`Clone`, `EffectiveLogAccumulationAlertRows` nil/zero→disabled), the
  `$mlog$`/`_MLOG$_DML_TYPE`/`_MLOG$_OLD_NEW` constants, and
  `MaterializedViewLogTableName` rune truncation against
  `mysql.MaxTableNameLength` → `table.rs`, each with its Go-exact JSON merge
  macro. Go's `append([]int64(nil), ...)` clone of an empty slice yielding
  nil is mirrored by `clone_like_go`.
- `bdr.go`: `SafeDDL` gains both new actions → `bdr.rs` `BDR_ACTION_ENTRIES`.
- `job_args.go`: `CreateMaterializedViewLogArgs` and `CreateMaterializedViewArgs`
  (`TableInfo`, `MLogTableIDs []int64`) with v1 `getArgsV1`/`decodeV1` and
  `GetCreateMaterializedViewLogArgs`/`GetCreateMaterializedViewArgs` →
  `job_args.rs` structs, `JobArgsValue` variants, identity/pointer/projection
  arms, `job_args_json.rs` Go-exact merge macros, and both getter functions.

## Known gaps recorded (not fixed in this batch)

- Go `SetTiFlashReplicaArgs.SkipColumnarStorageGate` (drift predating the
  `1c1a334d2b` pin) has **no Rust owner**: `SetTiFlashReplicaArgs` is not
  implemented anywhere under `rust/crates/` yet. The field belongs to the
  future `SetTiFlashReplicaArgs` transcreation, not to this drift batch.
- Go `TestJobSize`'s `unsafe.Sizeof(SubJob{})` 144→168 assertion is a Go
  memory-layout contract with no Rust equivalent (unchanged from prior
  receipts); `TestAddIndexArgs`'s `AutoPreSplit` additions already pass
  against the pre-existing aligned `IndexArg::auto_pre_split`.

## Regression tests

`rust/crates/tidb-model/src/tests_pkg_meta_model_materialized_view.rs`
(9 running tests), each citing its Go source:

- `create_materialized_view_may_need_reorg` (Go `TestMayNeedReorg`),
- `create_materialized_view_rollbackable_states` (Go
  `TestCreateMaterializedViewRollbackable`),
- `mv_action_names_and_bdr_safe_classification` (Go `job.go`/`bdr.go`),
- `create_materialized_view_args_roundtrip` (Go `TestCreateTableArgs`
  "create materialized view", both job versions),
- `create_materialized_view_log_args_roundtrip` (Go
  `GetCreateMaterializedViewLogArgs`),
- `table_info_mv_fields_omitempty_merge_and_clone` (Go `table.go` JSON
  shape + `TableInfo.Clone`),
- `mv_info_display_access_error_and_timezone_clone` (Go
  `TestMaterializedViewInfoClone` + `String`/`AccessErrorMessage`),
- `mv_log_rows_threshold_and_table_name` (Go
  `EffectiveLogAccumulationAlertRows`/`MaterializedViewLogTableName`),
- `subjob_involving_schema_info_persists_and_propagates` (Go
  `ToProxyJob`/`FromProxyJob` + omitempty/`json:"-"` contracts).

Fail-before evidence: with only the `may_need_reorg` and `is_rollbackable`
match arms reverted, both behavioral tests fail; the new-API tests bind to
symbols that do not compile against the pre-batch tree.

## Validation

Profile: **Ready** for this package batch.

```text
cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
RUST_MIN_STACK=67108864 cargo +nightly-2026-08-22 nextest run --offline --locked \
  -p tidb-model --no-fail-fast
# 321/321 passed (full owner suite, including the 9 new regressions)
OPENSSL_DIR=... cargo +nightly-2026-08-22 check --offline --locked \
  -p tidb-meta -p tidb-exec -p tidb-executor -p tidb-session -p tidb-domain
# 0 errors (JobArgsValue/Job/SubJob consumers)
```

No Go source changed in this batch. The `make lint` Go gate does not apply.
