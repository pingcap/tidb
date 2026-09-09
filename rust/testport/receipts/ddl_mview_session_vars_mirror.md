# Batch 25 — MV session-variable mirror wired to the live session

## Go reference (pinned master `94a9cbedab`)

* `AddMViewExecutionSessionVarsToJob` (`pkg/ddl/mview_worker.go`):
  snapshots the twelve MV-execution session variables into the DDL job
  envelope so the maintenance worker runs under the creator's settings.
* `CaptureMViewExecutionSessionVars` (pkg/sessionctx/variable): reads the
  live session values.

## Rust deliverables

* `rust/crates/tidb-session/src/vars.rs`:
  * `pub fn m_view_execution_session_vars_image(vars: &SessionVars)
    -> BTreeMap<String, String>` — captures the live values of the twelve
    MV-execution variables and renders them as the canonical
    (sysvar-name, value) pairs, reusing the same assignment builder the
    apply/restore machinery uses (one name list, one source of truth).
* `rust/crates/tidb-session/src/stmt_ctx.rs`:
  * `Session::ddl_statement_context` now installs the image via
    `StmtContext::set_session_vars_image`, so every DDL context the session
    builds carries the creator's live MV-execution settings.
* `rust/crates/tidb-exec/tests/cluster_ddl_source.rs`:
  * `materialized_view_lowering_follows_go_admission_order` now exercises
    the image-present branch of `add_mview_execution_session_vars_to_job`:
    a context whose image carries live values records THOSE in the job
    envelope (`tidb_mview_maintain_import_threads = 7`,
    `tidb_max_tiflash_threads = 16`), documenting that the image replaces
    the defaults wholesale when present.
* `rust/crates/tidb-session/src/tests_mview_session_vars.rs`:
  * new `session_vars_image_carries_live_values`: the image carries the
    canonical names mapped to live values (`tidb_isolation_read_engines`
    and `tidb_mview_maintain_import_threads` seeded), untouched knobs stay
    at registry defaults, and `Session::ddl_statement_context` carries the
    image forward.

## Validation

```
cargo +nightly-2026-08-22 nextest run -p tidb-session -E 'test(mview)'
# 13 passed (12 pre-existing + 1 new)
cargo +nightly-2026-08-22 nextest run -p tidb-exec \
  -E 'test(materialized_view) + test(persisted_materialized_view) +
      test(derives_the_purge_schedule) + test(preserves_text)'
# 12 passed
cargo fmt --all -- --check   # clean
```
