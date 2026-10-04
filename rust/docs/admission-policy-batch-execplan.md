# Complete shared account credential and certificate admission

This living ExecPlan follows root AGENTS.md and PLANS.md. Starting TiDB hparser-integration HEAD is 03065ea1ace65f55cd0e68a0055a3817e62257b5. Fresh Go master is 93a01d31f6da205ae4bf376825293903a6899fdb; freshly fetched integration remains e52af04b8c965c7bae2b35e91445c78261885dc0, already preserved. Native client-rust master remains 19a56ccda1e128218cd33c69709038219aced9bc and needs no edits. No push or dry run.

## Purpose / Big Picture


Accounts rotating passwords must authenticate the retained password with the same plugin verifier as the primary password. Accounts requiring cipher, issuer, subject or alternative names must enforce those properties of the actual verified peer certificate. A02/A03 are maintained together through the shared durable registry and opaque transport admission. This is existing-owner maintenance, not complete Go package acceptance.

## Progress


- [x] Read instructions, verify clean Cloud paths/heads, refresh requested branches and Go comparison.
- [x] Capture grouped runtime failures for secondary native/hashed credentials and specified TLS policies.
- [x] Connect secondary credentials to shared plugin verification, retaining invalid-primary guards, passwordless semantics and one failed-login update.
- [x] Carry actual verified certificate attributes/cipher into admission; accept and persist specified SQL policies through create/alter/reload/export/show.
- [x] Run grouped regressions, affected all-target checks, formatting, make lint and locked server build; self-review.
- [x] Update both registers/receipts and prepare the normal hook-gated local checkpoint plus recovery/startup instructions. Actual commit/SHA-dependent recovery/draft results are recorded externally in the final handoff; no push.

## Context and Orientation


The previous batch restored mysql.global_priv raw storage and X509 trust. rust/crates/tidb-server/src/configured_user_store.rs still verifies only authentication_string, despite retained additional_password in user attributes. rust/crates/tidb-session/src/privilege/registry_ops.rs always denies Specified policy; account.rs/account_password.rs refuse SQL certificate properties. mysql_tls.rs currently passes only verified-certificate presence. The same account registry and wire admission token must own these remaining checks; no second credential or TLS policy cache is introduced.

## Milestones and Plan of Work


First add grouped secondary-password and real-certificate regressions against old behavior. Then reuse the shared plugin verifier for primary and secondary hashes, exactly following Go's primary validity gate and empty-password path. Retained credentials remain in the already durable user-attributes image. A failed attempt updates lockout once after both checks; policy failure does not count as a wrong password.

Next extract negotiated cipher and the verified leaf certificate's issuer, subject and URI/DNS/IP names from the real rustls socket. Parsing certificate attributes is distinct from trust: rustls verification remains authoritative. Reuse tidb_util::tls cipher names and existing locked x509-parser dependency. Evaluate imported raw policies and SQL-created policies through the same registry owner. Go's SAN alternatives are OR within each type and AND across types; URI wildcards match only one nonempty whole path segment and preserve nonpath fields. Migrate CREATE/ALTER/SHOW consumers before retiring refusal assertions. TOKEN_ISSUER remains unsupported because its separate token authentication owner is absent.

Finally run one compatible grouped validation at the completed implementation boundary, all-target checks, format, lint and the mandatory locked build. Commit normally through hooks/pre-commit, which must repeat cd rust && cargo build --locked -p tidb-server. Refresh the verified bundle atomically and save exact Cloud heads/start instructions after full repository discovery. No push.

## Concrete Steps


Every shell sources /workspace/.cloud-setup/env.sh. Cargo cwd is /workspace/tidb/rust. Baseline and final filters are admission_policy_batch across tidb-server library and configured_user_store_source standalone target; combine existing account/TLS regressions afterward. Select server aggregate separately for TLS integration rather than requesting every package's aggregate. Check free disk before linking and journal only proven obsolete or completed test outputs. Protect current Cargo build-directory outputs as well as published binaries. Logs live in /workspace/.cloud-setup/admission-policy-batch.

## Validation and Acceptance


Native, caching_sha2 and sm3 secondary passwords must use the account plugin and matched identity, including blank primary, malformed primary/secondary and wrong-password controls. Successful fallback resets lockout once; failed fallback increments once. TLS-only, absent and untrusted certificates cannot satisfy Specified policy. Matching verified issuer/subject/cipher/SAN succeeds; every mismatched property denies; multiple SAN values preserve Go alternatives and URI segment rules. CREATE/ALTER policy survives typed storage/export/reload and SHOW prints Go order. Password-only ALTER retains TLS policy; drop/rename use the same durable rows. Full certificate reload/rotation, durable wire-login counters, mixed-node security, complete packages and performance remain unverified.

## Idempotence and Recovery


Preserve all local/concurrent changes and the current verified bundle until a replacement verifies. Do not reset, rewrite origins, push, force-push or bypass hooks. Do not delete useful tests to pass. Correct stale specified-policy refusals while retaining malformed-policy and mismatch coverage. Fresh-task restoration remains unverified until independently demonstrated.

## Interfaces and Dependencies


PrivilegeRegistry reads retained credentials and raw TLS requirements from existing storage owners. Certificate attributes are a typed immutable value carried only by the crate-private transport admission token from verified socket state. SQL options serialize into existing global_priv rows and all admission paths use shared registry validation. x509-parser is already in the workspace lockfile; any direct-dependency lock change must preserve existing versions. Native sync is unnecessary because native source remains unchanged.

## Surprises & Discoveries


Fresh remote refs are unchanged. The first full credential run exposed three subsequent failures from a persisted-secure-transport test that did not restore its process global; adding the existing RAII guard makes all 19 cases pass. Disk exhaustion during an auxiliary-bin link is excluded from semantic evidence; only superseded server graphs lacking the new x509 dependency, completed disposable tests and terminated-linker temporary files are reclaimed with journals. Other 55 unresolved IDs retain prior evidence rather than fresh behavioral reproduction. Cluster identity/remote KILL requires its complete TiDB coprocessor route and lease owner; it is not a shortcut through an invented HTTP endpoint.

## Decision Log


Decision: complete credential fallback and specified certificate checks as one account-admission batch. Rationale: both are residual consumers of the same durable account image and must run before the existing lockout/expiry completion path. Date: 2026-10-04.

## Outcomes & Retrospective


Seven runtime baseline failures are captured: three real-certificate policies and four original-verifier credential cases. Initial fixture compilation failures, two contaminated credential passes and a disk/link failure are excluded. Implementation and grouped validation are complete: 126 distinct Rust cases plus 52 Go URI plus three Go JSON oracle cases, all-target checks, formatting, make lint and the explicit locked build pass. A public-SQL self-review probe additionally failed on Go field order/escaping; all 44 affected session/account cases then pass after correction against three Go JSON oracle cases. The actual normal precommit is mandatory. Final SHA-dependent commit/recovery/draft results are recorded in /workspace/.cloud-setup/admission-policy-batch/final-handoff.json. Broad A02/A03 findings remain partial until their named wider owners are complete.
