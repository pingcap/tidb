# `pkg/ddl` DROP INDEX child clustered-handle parity receipt

Status: completed Rust-only alignment for Go's child-side clustered-handle
exemption in `checkIndexNeededInForeignKey`. Go authority is
`origin/master` at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`; the complete
recursive `pkg/ddl` inventory remains in
`receipts/ddl_foreign_key_owner_inventory.md`.

Go runs one shared check over declared and referred foreign keys
(`pkg/ddl/foreign_key.go:443-487`). When the dropped index covers a single
constrained column that is also `PKIsHandle`, the row handle is sufficient and
the drop is accepted even if no secondary index remains. Rust previously
applied this escape only while scanning the referred-parent branch, so Go's
`foreign_key_test.go:920` child-cover pass shape incorrectly returned 1553.

`foreign_key::check_index_needed` now uses the same escape on the declared-child
branch. The clustered handle remains the only new coverage source; ordinary
child covers still require a surviving index, and the check remains independent
of `foreign_key_checks`.

Focused regressions:

- `tidb-executor::fk_alter_meta_and_privilege_source::dropping_the_child_handle_cover_is_allowed`
  executes the source-shaped DDL, verifies `DROP INDEX idxb` succeeds, the
  explicit index is gone, and the foreign key remains.
- `tidb-session::tests_foreign_key::the_clustered_handle_exemption_allows_the_child_cover_drop`
  verifies the SQL-visible index and constraint state.
- Existing `the_clustered_handle_exemption_does_not_reach_the_child_index`
  remains the control proving a non-handle child index still returns 1553.

No Go, generated, platform, Bazel, or module files changed. Partial-index
predicate safety and multi-action ALTER atomicity remain neighboring explicit
boundaries.
