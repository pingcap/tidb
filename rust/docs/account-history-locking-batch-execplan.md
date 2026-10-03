# Preserve account history and failed-login policy


This living ExecPlan follows root PLANS.md. It repairs existing account owners;
it does not accept pkg/executor or pkg/privilege/privileges as fully transcreated
packages. The complete package artifact and validation obligations in the current
audit remain in force.

## Purpose / Big Picture


PASSWORD HISTORY and PASSWORD REUSE INTERVAL must reject protected credentials
before changing a password. Their nullable user policy and timestamped history
must survive the server's snapshot, account transaction and publication path.
Failed-login policy and stored lock epochs must survive that same path without
discarding unrelated user attributes. These repair A04 and part of A02 together.

## Context and source


Work only in /workspace/tidb, hparser-integration. Go master freshly fetched is
93a01d31f6da205ae4bf376825293903a6899fdb; native client remains
19a56ccda1e128218cd33c69709038219aced9bc. The remote advanced to ee637c3a3992
with schema-acknowledgement changes and was merged without source conflicts.
Keep those changes. Existing local account commits are 05ed8bc460 and ee74230b33.

Read Go pkg/executor/simple.go's option loader, getUserPasswordLimit,
passwordVerification, deleteHistoricalData, CREATE/ALTER/SET/DROP/RENAME callers,
and pkg/privilege/privileges/privileges.go::PasswordLocking.ParseJSON. No doc.go
exists in these directories. Rust Session owns policy, tidb-exec owns stored rows,
and tidb-server connects the registry to its durable account transaction. Extend
these owners rather than introducing another account SQL interpreter.

## Progress


- [x] Refreshed master, inspected live owners, preserved remote schema changes.
- [x] Reproduce history no-op and lost locking policy through real unistore SQL.
- [x] Carry policy, history and raw attributes through all account image callers.
- [x] Enforce count/time policy and migrate CREATE/ALTER/SET/DROP/RENAME and mirrors.
- [x] Validate red/green regressions, targeted suites, all-target check, lint.
- [x] Update both registers/receipts; source commit 4b1aebe025 through actual hook; fresh locked build immediately before normal push attempt.
- [ ] Publish to requested remote after GitHub write access is restored (current attempt denied HTTP 403).

## Implementation milestones


First add SQL regressions using the existing cop_backed_stack fixture in
tidb-server cluster_session_node/tests/unistore_cop.rs. Expect reuse to succeed
before repair and stored locking JSON to be absent. Retain exact failing logs in
/workspace/.cloud-setup/account-batch3.

Extend LoadedUser/ClusterPrivileges and registry export with nullable reuse policy,
raw user attributes and typed history timestamps. Use UTC row decoding and the
existing system-row encoding/index owner. Preserve orphan history rows unless
DROP or RENAME names that account. Preserve NULL versus zero policy and unrelated
JSON. Parse stored locking values with Go clamps and reject malformed epochs.

Enforce Go's union of recent-count and protected-time windows before mutation;
apply statement overrides and live global defaults. Exempt empty encoded credentials and
Go-exempt plugins; salted plugins encode empty plaintext as a nonempty credential. Keep salted verifier checks. Prune only rows outside both
windows, append history and clear history on plugin changes. Account statements
must carry the same policy through shared registry and local table mirrors.

Run source /workspace/.cloud-setup/env.sh before Cargo. Required gates are scoped
Rust account tests, real unistore SQL, cargo check --locked -p tidb-exec -p
tidb-session -p tidb-server --all-targets, make lint, and git diff --check. Commit
with core.hooksPath=hooks and verify its actual locked server build. Immediately
before every push run cd rust && cargo build --locked -p tidb-server. Push only
origin HEAD:hparser-integration, never force. GitHub's existing 403 remains an
external publication blocker; preserve a verified bundle if still denied.

## Surprises & Discoveries


The remote changed independently in schema synchronization. The merge preserved
both histories. Git merge does not invoke pre-commit, so the actual hook was also
executed using an alternate index containing the merge delta; its locked build
passed. Record this separate execution accurately.

## Decision Log


Keep this batch in the existing account storage/Session owners. Shared dependency
cache replacement requires whole Ristretto acceptance before activation and is
therefore a separate, larger prerequisite batch. This batch does not enable TLS
certificate policies or claim persistent wire-login counters before the auth
transaction owner is composed.

## Outcomes & Retrospective


The durable history and locking image are implemented. The Session account/grant
suite passes all 115 tests. Final SQL, affected all-target checking, lint and the actual staged-hook build
pass. The 156 distinct targeted tests pass. Source commit 4b1aebe025 ran the
actual hook; the fresh pre-push build passed in 21.06 seconds. Push exited 128
with HTTP 403, remote verified at ee637c3a39. Publication remains externally
blocked; code and recovery bundle are retained in the cloud workspace.
The register starts with 86 findings,
66 unresolved (56 open, 10 partial), 20 repaired. Change statuses only for the
contracts demonstrated by production-path regressions; preserve valid failures.


### Validation discoveries, 2026-10-03

A controlled baseline run (batch patch reversed and restored in a finally block)
reproduced 31 older account/grant failures: 80 passed. New history diagnostic
coverage caught an additional formatter bug: the MySQL catalog lacks TiDB code
3638. Use the existing TiDB catalog and shared NewErrf formatter. No diagnostic
template is duplicated. The final Session suite passes 115 tests.

Go master formatAccountName uses stringutil.Escape (backticks by default), so
106 obsolete SHOW GRANTS output literals were replaced while preserving input
SQL and behavior assertions. Old kill fixtures used odd 32-bit IDs, which global
kill treats as truncated; use valid even IDs. PROCESS exposes metrics_schema in
Go DBIsVisible; update that assertion and the current system-schema inventory.
No test functions were deleted or disabled. Two real failures were retained and
fixed: composeGlobalPrivUpdate includes static CREATE/DROP ROLE in ALL, and
setDataFromSchemata applies RequestVerification to every schema. Synthesize rows
through that same existing visibility owner.

Unchanged history TEXT values initially compared encoded Datum::String with
Datum::Bytes and caused needless rewrites. Compare payload bytes before invoking
the existing typed row encoder. Standalone Session bootstrap also required its
existing mysql.password_history schema. Both owners were repaired.

make lint initially returned zero while revive reported missing module downloads:
Go proxy redirected large archives to a denied storage.googleapis.com endpoint.
Recover exact upstream GitHub tags through codeload, create module archives with
golang.org/x/mod/zip, and require go mod download to validate existing go.sum
checksums. Do not disable checksum verification or change lockfiles.

This plan was updated after implementation to record real failures and Go-based
test maintenance; package/platform/security acceptance remains separately open.


Self-review found a timezone error in the standalone SQL mirror: UTC history text
was inserted into a session-local TIMESTAMP. An added +08:00 assertion failed
with a value eight hours behind; convert the immutable registry timestamp into
the session zone before insertion. Durable cluster rows remain UTC.

CREATE's Go loop excludes only auth-token history, whereas ALTER/SET's reuse
checker also exempts LDAP. Preserve that difference. Do not treat all plugins
as having identical creation and mutation history rules.

The reconstructed lz4 archive failed its existing go.sum checksum and was never
used. Packaging the exact Git tag with x/mod/zip.CreateFromVCS reproduced the
recorded checksum, and Go accepted it. Other four module archives also matched
their recorded checksums. Final make lint exited zero without dependency errors.

Final receipt update: record the actual source commit, hook/build logs, push exit
and unchanged remote SHA. The following documentation-only commit preserves
these outcomes; no source implementation or test obligation changes.
