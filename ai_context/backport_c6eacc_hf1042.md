# HF-1042 backport context for `c6eaccfb0a1b5566bcc642db8d6932e6f58e9b45`

## Source context

- Upstream issue: `pingcap/tidb#68184`
- Upstream PR: `pingcap/tidb#68186`
- Upstream merge commit on `master`: `c6eaccfb0a1b5566bcc642db8d6932e6f58e9b45`
- Upstream PR title: `*: restore views without placeholder tables`

That upstream change was merged into `master` on 2026-05-26. It fixes Lightning view restore when dumpling emits placeholder table schema files for views, especially when downstream runs with `sql_require_primary_key=ON`.

## Hotfix branch context

- Current hotfix branch: `hotfix/HF-1042/v6.5.11`
- This branch was created from tag: `v6.5.11-20241210-90c735f`
- The tag points to commit: `90c735f56125ed8eebefdee406fa18268f0b5a6c`

This means the work here is a backport from upstream `master` to the 6.5.11 code line, not a same-branch cherry-pick.

## Why this was handled as a backport instead of a direct cherry-pick

I did a dry run with:

```bash
git cherry-pick -x --no-commit c6eaccfb0a1b5566bcc642db8d6932e6f58e9b45
```

The dry run showed that a direct cherry-pick was not the right approach.

Main reasons:

- Upstream code lives under `pkg/lightning/mydump/...`.
- In `v6.5.11`, the active loader code is under `br/pkg/lightning/mydump/...`.
- More importantly, the active schema/view restore path in `v6.5.11` is in `br/pkg/lightning/restore/restore.go`, not in the newer upstream `schema_import.go` path.
- The cherry-pick conflicts were not only path-renames. They also reflected real architecture drift between `master` and `v6.5.11`.

So the correct approach here was:

1. Treat upstream commit `c6eaccf...` as the semantic source of truth.
2. Re-implement the same behavior on top of the 6.5 code paths.
3. Keep the behavior change, not the exact upstream file layout.

## Backport strategy used

The upstream fix was split conceptually into two parts and adapted onto 6.5:

### 1. Loader-side placeholder table pruning

Files:

- `br/pkg/lightning/mydump/loader.go`
- `br/pkg/lightning/mydump/loader_test.go`

Changes:

- View schema files are no longer rejected just because a matching placeholder table schema is missing.
- View metadata is tracked separately with a dedicated `viewIndexMap`.
- After all schema files are discovered, tables whose names collide with views are pruned from `MDDatabaseMeta.Tables`.
- This removes dumpling-generated placeholder tables from the table restore plan before Lightning attempts to restore schemas.

Why:

- Without this, Lightning still tries to restore placeholder tables for views.
- Under `sql_require_primary_key=ON`, those placeholder tables can fail before the actual view restore stage.

### 2. Restore-side dependency-aware view creation

Files:

- `br/pkg/lightning/restore/restore.go`
- `br/pkg/lightning/restore/view_restore.go`
- `br/pkg/lightning/restore/view_restore_test.go`

Changes:

- Added view SQL parsing and dependency extraction for dumped views.
- Added CTE-aware dependency collection so CTE names are not misclassified as real tables/views.
- Added topological ordering for dumped views, including cross-database dependencies.
- Added validation for external dependencies that are not part of the current dump.
- Added downstream object-type lookup via `information_schema.TABLES` so the restore path can distinguish existing views from existing non-view objects.
- Filtered dumpling cleanup DDL (`DROP TABLE IF EXISTS`, `DROP VIEW IF EXISTS`) out of the execution plan for views.
- Replaced the old naive "restore views one by one in file order" path in `restore.go` with the dependency-aware plan.

Why:

- The old 6.5 behavior restored views sequentially but without dependency planning.
- It also relied on dumpling placeholder tables being restored first.
- The upstream fix removes that assumption, and this backport reproduces the same behavior on the 6.5 restore stack.

## Conflict resolution / adaptation notes

### Code layout conflict

The biggest "conflict" was architectural, not textual:

- upstream: `pkg/lightning/mydump/schema_import.go` and `pkg/lightning/mydump/view_import.go`
- v6.5.11: `br/pkg/lightning/mydump/loader.go` and `br/pkg/lightning/restore/restore.go`

Resolution:

- Do not try to force the upstream files into the 6.5 tree.
- Implement the equivalent logic inside the 6.5 loader and restore paths.

### Downstream object type detection

In 6.5, `FetchRemoteTableModels` is not sufficient to distinguish base tables from views for this purpose.

Resolution:

- Query `information_schema.TABLES` directly during view restore planning.
- Use `TABLE_TYPE` to classify existing downstream objects as view vs non-view.

### Old restore schema test fixtures became self-referential

`br/pkg/lightning/restore/restore_schema_test.go` originally created fake views named `tbl1`, `tbl2`, ... and each view selected from the table of the same name.

That becomes a true self-dependency once placeholder tables are pruned.

Resolution:

- Rename the fake test views to `view1`, `view2`, ... while keeping them dependent on `tbl1`, `tbl2`, ...
- This preserves the purpose of the suite while making it compatible with the new dependency model.

### Context-cancel assertion in restore schema test

The context-cancel path now returns a wrapped error.

Resolution:

- Update the assertion to use `errors.ErrorEqual(...)` instead of strict equality.

## Tests and verification performed

### Passed

- `go test ./br/pkg/lightning/mydump`
- `go test ./br/pkg/lightning/restore -run '^$'`
- `go test ./br/pkg/lightning/restore -run '^(TestParseViewSchemaSQL|TestParseViewSchemaSQLIgnoresCTEDependenciesInSetOperatorRoot|TestBuildViewImportPlanSupportsCrossSchemaDependencies|TestRestoreSchemaWorkerEnqueueViewJobs|TestRestoreSchemaSuite)$'`
- `bash -n br/tests/lightning_view_dependency/run.sh`

### Added / updated test coverage

- Loader tests now cover accepting standalone view schema files and pruning placeholder tables from `Tables`.
- New restore tests cover:
  - filtering placeholder cleanup DDL from view schema SQL
  - CTE-aware dependency extraction
  - cross-schema dependency ordering
  - ordered view job execution through the 6.5 restore worker
- New BR integration case added:
  - `br/tests/lightning_view_dependency`

### Not run

I did not run the new `br/tests/lightning_view_dependency` integration case end-to-end because the local environment does not currently have the required BR integration binaries available under `bin/`:

- `bin/tidb-server`
- `bin/tikv-server`
- `bin/pd-server`

So the integration asset was added and shell syntax-checked, but not executed locally.

## Practical summary

This backport intentionally does not preserve the upstream file layout. It preserves the upstream behavior on the 6.5.11 codebase:

- no placeholder table restore for dumped views
- dependency-aware view restore order
- compatibility with `sql_require_primary_key=ON`
- safer handling of existing downstream objects

If someone later compares this branch against `c6eaccf...`, they should expect semantic equivalence with 6.5-specific implementation details, not a textual cherry-pick.
