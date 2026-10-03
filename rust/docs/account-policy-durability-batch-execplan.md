# Preserve account policy and enforce password reuse through the shared account owner

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective current according to root PLANS.md.

## Purpose / Big Picture

Repair related account-state findings A02 and A04 together. A policy read from
mysql.user/mysql.global_priv must survive registry publication and account writes.
CREATE/ALTER USER and SET PASSWORD must persist and enforce Go's password history
and reuse interval through that same state, rather than silently accepting options.
Use Go master 93a01d31f6da205ae4bf376825293903a6899fdb, integration handoff
b6520ebe83c992a34cf75dc7e381b1eef20e288f and unchanged native client
19a56ccda1e128218cd33c69709038219aced9bc. Recheck remote refs before publication.
This maintains existing account packages; broad executor/privilege package
acceptance and TLS transport A03 remain separately open.

## Progress

- [x] Prepare cloud tools and validate locked server build, actual commit hook,
  native library tests, Go race tests, native sync regeneration and live SQL.
- [x] Recheck current A02/A04 source evidence and Go password-reuse algorithm.
- [x] Compare all 66 carried findings with prior source hashes and review intervening owner changes.
- [x] Remove the Go-contradicting empty-account discard test and production discard; prove failure before and 14 cluster tests passing afterward.
- [x] Repair three stale hint test API calls and Go-observed EOF diagnostics; four hint tests pass.
- [x] Complete this maintenance batch's account source/test/caller accounting and fail-before regressions; parent package acceptance remains open.
- [x] Carry expiry lifetime/timestamp through read, the single shared record, export and typed account mutations; retain NULL/NEVER and original epochs.
- [x] Carry locking state, raw attributes and history through the same bridge.
- [ ] Carry global_priv/TLS and remaining authentication consumers through it.
- [x] Integrate CREATE/ALTER/SET/DROP/RENAME callers and enforce Go reuse rules.
- [x] Run affected regressions, source-based contract review, affected Ready checks and lint; full original Go suites remain unverified.
- [x] Update both finding registers and durable receipt with accurate limits.
- [ ] Commit through actual hook; fresh locked build immediately before normal
  push, verify remote SHA or record precise permission blocker.

## Surprises & Discoveries

The cloud platform initially checked TiDB out on work at Go master. The editable
branch is now hparser-integration and was clean at the handoff. Git reads work;
TiDB push dry run reports `Permission to pingcap/tidb.git denied to ngaut.` and
HTTP 403. The user will arrange access; retain the requested destination.
Native push dry run succeeds. Setup is saved but publication is product-owned.

The optional parser integration tests do not compile on unchanged HEAD: three
parse_hint calls omit its fourth usize argument. Do not represent them as passed.
Direct server startup needs RUST_MIN_STACK=33554432, exported by cloud env.sh.
The unistore load-privileges mode attempts an empty auth-file path; the supported
mode-0600 local auth-file development mode passes repeated SQL checks.

## Decision Log

- Decision: Batch account durability and reuse policy because they read and write
  the same durable account state. Keep native PD discovery/transport work separate
  until its shared prerequisites and complete ownership scope are ready.
  Rationale: Independent security correctness is explicitly allowed by the full
  structural plan and must not wait for unrelated performance/PD work.
  Date/Author: 2026-10-02 / Codex Cloud continuation.
- Decision: Preserve Go's nullable per-user policy versus live global defaults,
  salted-password comparisons, empty-password exclusions, history/time union,
  plugin changes and transactional account-write boundary. Preserve unknown
  attributes and existing timestamp/lock epochs during reconstruction.
  Rationale: A local history vector or display-only option does not repair the
  cluster storage and authentication contract.
  Date/Author: 2026-10-02 / Codex Cloud continuation.

## Context and Orientation

Cloud source roots are /workspace/tidb and /workspace/client-rust. Activate tools
with source /workspace/.cloud-setup/env.sh in every shell; do not create worktrees.
Go master is a separate plain export at /workspace/.cloud-setup/go-master.
Master pkg/privilege/privileges/cache.go owns decoding/security policy;
pkg/executor/simple.go owns passwordReuseInfo, passwordVerification,
checkPasswordReusePolicy, history retention and account statement transactions.
Its loadOptions distinguishes unspecified/default from explicit zero and clamps
history/interval counts to MaxUint16. The existing Rust shared registry is in
crates/tidb-session/src/privilege.rs and its submodules. Account statement callers
are account.rs/account_password.rs/user_table.rs. Cluster DTO loading/writing is
crates/tidb-exec/src/{cluster_privilege_load,cluster_account_write}.rs;
crates/tidb-server/src/cluster_privileges.rs bridges the DTO and registry. Follow
the existing staged transaction owner; do not introduce a competing SQL executor.

## Plan of Work

Inventory the complete upstream package obligations using existing audit inventories,
read original tests and recheck every live caller. Add failing roundtrip tests for
SSL/expiry/locking/anonymous policy and SQL history/reuse tests before production
edits. Extend shared account state and its existing storage bridges together;
retain nullable values and unknown JSON attributes, and use existing credential
verification for native and salted plugins. Check/trim/add history before credential
publication using Go's count/time rules in the account transaction. Integrate every
credential creation/change, plugin reset, drop and rename caller. Preserve rollback
and concurrent cluster conflict handling. Recheck login expiry/locking, unknown or
unsupported TLS policies, and both local and delegated account storage modes.

## Concrete Steps

From /workspace/tidb/rust, use cargo test --locked -p tidb-server --lib with the
cluster_privileges filter and cargo test --locked -p tidb-session --lib with the
tests_grants filter; select narrower tests during iteration. Read actual target
names before invocation. Run affected tidb-exec account bridge tests through its
existing all aggregation target. Keep logs under /workspace/.cloud-setup/account-batch.
Run source Go tests from the exact master export with readonly modules and required
failpoints per docs/agents/testing-flow.md, never mutate the module cache.
At delivery run affected all-target checks, scoped rustfmt, root make lint and
git diff --check; distinguish diagnosed baseline failures from new failures.

## Validation and Acceptance

Go-authored policies survive load/export/write/reload without replacing epochs.
History 3 rejects reuse across ALTER USER and SET PASSWORD; policy DEFAULT reads
current globals, explicit zero disables the corresponding rule, and expired rows
outside both windows are removed. Empty encoded credentials never create history; salted plugins may encode empty
plaintext as a nonempty credential and compare plaintext with stored hashes, plugin changes clear incompatible
history, drop/rename follows Go, and rejected changes retain prior credentials.
Original cases and cloud process SQL checks substantiate each accepted behavior.
Do not close A02/A04 until their entire recorded contract passes; keep broad package
and untested multi-node/platform obligations explicit. Claim no workload speedup.

## Idempotence and Recovery

Preserve concurrent user files/commits and refresh refs safely. Use disposable test
databases. Retain failing logs. Stage only reviewed files, never bypass hooks or
force-push. A permission denial does not block independent implementation/tests.
The actual hook must run cd rust && cargo build --locked -p tidb-server, and the
same command must pass again immediately before every future push.

## Artifacts and Notes

Setup and activation: /workspace/.cloud-setup/{install.sh,env.sh}; current-instance
logs and smoke driver live there. Prior local receipt paths are historical evidence,
not available cloud files. This batch's final receipt must use real cloud paths and
command exit statuses, test counts and source pins.

## Outcomes & Retrospective

Implementation and final batch acceptance are pending. Environment setup is
validated and saved for review; TiDB remote write authorization remains unresolved.

### Cloud test-hygiene prerequisite checkpoint

The user requested removal of stale/harmful tests. One existing test required
incorrect empty-account loss and was replaced alongside the loader repair.
Three API-stale hint calls were updated; their valid assertions exposed a real
Go diagnostic mismatch, which was repaired rather than deleted. This checkpoint
repairs the anonymous-row subitem of A02, not the complete A02/A04 contract.
Exploratory expiry/locking/history red probes confirm the remaining work and are
not committed as passing coverage. Broader parser library validation reports
719 passed and 16 failed; those tests remain. See current-audit/cloud-account-test-review.md
and its JSON receipt for exact pins, validation and publication limits.

### Expiry durability milestone


The first substantive account-state milestone is complete: stored password
lifetime and change epochs survive publication and write/reload. UTC decoding
is explicit; the registry owns one typed timestamp and derives login time from it.
Old epochs, live defaults, NULL versus NEVER and zero timestamps are covered.
Publication and insertion regressions fail before their repairs and pass after.
This removes the expiry subitems of A02 while retaining its TLS/locking/attribute
gaps and all of A04. No new history implementation is dispatched and no whole
upstream package is accepted. See parity/current-audit/account-expiry-durability-repair.md.
Next milestone remains complete account attributes/global_priv and history
ownership, then every CREATE/ALTER/SET/DROP/RENAME caller and transaction gate.


### History and locking-image milestone, 2026-10-03

The shared account image now carries nullable history/reuse limits, timestamped
history and complete raw user attributes. Locking limits, counts and original
epochs survive read/publication/write/reload; metadata and secondary credentials
are retained. CREATE/ALTER/SET/DROP/RENAME compose the shared history owner, which
checks Go count/time windows before credential publication. A04's recorded no-op
contract is repaired; A02 remains partial for mysql.global_priv/TLS, durable wire
login counters and broader cache invalidation. See account-history-locking-batch-
execplan.md for current validation and parity/current-audit/account-history-
locking-repair.md for the final receipt. No complete Go package is accepted.
