# Restore session migration through shared owners

This living ExecPlan follows `PLANS.md` at the repository root.

## Purpose / Big Picture


Repair the missing session migration flow across S03, N03 and S04 against Go master 93a01d31f6da205ae4bf376825293903a6899fdb. A connection must export its state, restore it on another connection, and retain variables, prepared statements and bindings. Migrated historical timestamps must reach the existing historical-read owner and read-only guard. This batch does not claim complete transcreation of the sessionstates, session or variable Go packages.

## Progress


- [x] Verify clean Cloud checkouts and refreshed upstream refs.
- [x] Trace Go export, restoration and protocol ownership.
- [x] Add grouped failing regressions in the existing session and protocol suites.
- [x] Compose state serialization, admission, variable and prepared/binding owners.
- [x] Validate the combined batch, update both finding registers and self-review.
- [x] Prepare validated changes for normal commit/publication with enforced hook and prepush gates. Actual command results belong to `/workspace/.cloud-setup/session-migration-batch/final-handoff.json`; a missing successful receipt means publication is pending.

## Surprises & Discoveries


Datum and FieldType already have Go JSON encoders. Reuse these rather than introducing a parallel format. SQL and binary prepared statements currently allocate and retain state separately; migration requires preserving their shared ID namespace. Session tokens require a signing certificate owner; an unconfigured Go server returns a NULL token.

## Decision Log


Use existing statement dispatch and variable hooks. Restore warnings after handlers because preparing statements and setting variables can alter them. Keep full-package acceptance distinct from repair evidence: certificate lifecycle, hypothetical indexes and any unimplemented prerequisites remain explicit obligations.

## Context and Orientation


`rust/crates/tidb-session/src` owns SQL state and bindings. `rust/crates/tidb-server/src/mysql_connection.rs` owns binary prepared buffers and cursors. Both server stores wrap the same Session. The Go reference is `/workspace/.cloud-setup/go-master`, especially `pkg/session/session.go`, `pkg/sessionctx/sessionstates`, `pkg/server/driver_tidb.go` and `pkg/bindinfo/session_handle.go`.

## Plan of Work


First extend existing session regression tests to show export and restore currently fail. Add state composition beside Session, preserve prepared SQL and prepare-time database, and share migration variables with existing typed hooks. Integrate the binary protocol owner without copying physical plans or retaining cursors. Remove inaccurate prepared-owner comments as their assumptions change.

## Concrete Steps


From `/workspace/tidb/rust`, source `/workspace/.cloud-setup/env.sh` and run focused `cargo test --locked -p tidb-session --lib session_migration` before and after changes. Run related prepared, variable and historical-read suites together after implementation. Run server protocol regressions, all-target checking and `make lint`. The actual pre-commit hook must run `cargo build --locked -p tidb-server`; rerun it immediately before any push and verify remote HEAD. Never force push.

## Validation and Acceptance


Transfer state between two sessions sharing a catalog. Verify typed user variables, system-variable dependencies, database, warnings, sequence values, prepared statement identity and bindings. Refuse active transactions, local temporary tables, advisory locks, bound long data and unconsumed cursors. Confirm failed historical state restoration preserves the prior snapshot. Tests must show failures on the baseline and pass after changes. Report unsupported obligations without marking their roots repaired.

## Idempotence and Recovery


All tests use isolated sessions. Preserve concurrent work; do not reset checkouts. Keep logs outside source under `/workspace/.cloud-setup/session-migration-batch`. A failed validation stops publication, not independent repair work.

## Artifacts and Notes


Starting TiDB HEAD is 3cb7eaec4b2b230a0beb9ecebe127094d8a05279. Native HEAD is bc8cca3fea4f741e68a41b124f28164604a57ee2. No native source change is planned.

## Interfaces and Dependencies


Use existing Datum JSON, SessionVars, PreparedStore, SessionBindings and QuerySession owners. Do not introduce an alternate SQL parser or restore compiled plans across sessions.

## Outcomes & Retrospective


337 targeted session tests and 20 serial MySQL wire tests pass, as do all-target checks and make lint. S03 advances from open to partial; N03/S04 remain partial. The register has 86 findings: 30 repaired, 28 open and 28 partial. Full Go package acceptance, signed tokens, exact imported user-variable types and the other obligations in the validation JSON remain open. Native source is unchanged.

The protocol matrix also exposed and repaired shared grant/revoke result classification and SQL-mode escaping. Four regressions fail against the relevant baseline owners. A prepared SHOW panic exposed missing retention for complete row results; the shared owner now retains them. Protocol tests that mutate process-wide TLS settings run serially. Test cleanup removes the synthetic grant-resultset helper and the unrelated OUTFILE unsupported assertion, while preserving Go-backed obligations.
