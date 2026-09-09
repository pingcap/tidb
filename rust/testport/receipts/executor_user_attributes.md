# `pkg/executor` — USER_ATTRIBUTES visibility consumer receipt

Status: bounded Rust consumer-alignment batch; this is not a complete
transcreation of the large Go package. Comparison source: Go
`origin/master` at `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5` (2026-09-03).

## Inventory and boundary

Before editing, the complete direct `pkg/executor` inventory was read from the
comparison commit: 173 artifacts and 103,117 lines, including every
production/test Go file, `BUILD.bazel`, and `OWNERS`. Recursive executor
subpackages, generated/platform variants, fixtures, and build inputs were
checked as package-boundary evidence; no generated or platform-specific
artifact was changed. The captured `rust/difftests/infoschema/show_create_table.txt`
fixture was updated with the new table block. The root package has no `doc.go`.
The Go source
authority is `pkg/executor/infoschema_reader.go` together with
`pkg/infoschema/tables.go` and `pkg/privilege/privileges/user_attributes_filter.go`.

## Implemented behavior

`tidb-executor` now registers the `INFORMATION_SCHEMA.USER_ATTRIBUTES` schema
with Go's three columns (`USER`, `HOST`, `ATTRIBUTE`). `tidb-session` now
materializes rows by selecting `User`, `Host`, and the JSON `metadata` member
from the real `mysql.user` catalog table, then applies the Go/MySQL 8.0.22
visibility matrix:

- SELECT or UPDATE on `mysql.user` (or `mysql.*`) sees every row;
- CREATE USER plus `SYSTEM_USER` sees every row;
- CREATE USER without `SYSTEM_USER` sees non-system rows only; and
- all other authenticated viewers see their own account row only.

Nil identity/privilege state and the explicit privilege-bypass path retain the
existing unrestricted fallback. No new privilege facade or Go code was added;
the consumer uses the existing Rust `PrivilegeRegistry` host matching and
dynamic-privilege APIs.

## Regression and validation

`user_attributes_rows_follow_go_visibility_rules` covers ordinary, SUPER-only,
SELECT-on-`mysql.user`, SELECT-on-`mysql.*`, CREATE USER, and SYSTEM_USER
viewers, and verifies that the unquoted JSON metadata is returned. Before the
Rust consumer/schema branch existed, the focused query failed with
`Schema(UnknownTable("INFORMATION_SCHEMA.user_attributes"))`; after the fix it
passes.

The Ready validation evidence for this batch is recorded in the handoff:

- focused session regression:
  `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --lib user_attributes_rows_follow_go_visibility_rules -- --nocapture --test-threads=1`;
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-session --all-targets` — passed;
- the focused `tests_grants::visibility` module — 11 passed;
- Rust formatting and `git diff --check`; and
- repository `make lint` with the pinned Go toolchain.

The full `tidb-session --lib` run remains a shared-worktree baseline boundary:
1,240 tests passed and 274 unrelated harvested/planner/partition/JSON tests
failed. The existing all-served-table SHOW CREATE fixture is also missing nine
older served-table blocks; the new `USER_ATTRIBUTES` block itself matches.

`make bazel_prepare` is not required because this batch changes no Go source,
imports, tests, module files, or Bazel metadata.

## Risks and unverified surfaces

The reader depends on the existing Rust privilege registry and the bootstrapped
`mysql.user` catalog shape; duplicate account rows and future privilege-cache
differences remain separate parity boundaries. This batch does not certify the
full executor package, generated/platform builds, or integration suites.
