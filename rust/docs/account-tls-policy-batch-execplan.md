# Restore the shared account TLS policy lifecycle

This living ExecPlan follows root PLANS.md and AGENTS.md. Work begins at /workspace/tidb hparser-integration 399de17d8b4f9f9f3594917747939de36b8f0a6c. Freshly fetched integration remains e52af04b8c965c7bae2b35e91445c78261885dc0 and Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb. Native /workspace/client-rust master is unchanged at 19a56ccda1e128218cd33c69709038219aced9bc. No push or dry run.

## Purpose / Big Picture

An account's REQUIRE policy must survive cluster reload and account writeback, and inbound MySQL TLS must verify a presented certificate under the configured CA before authentication can use it. Repair the connected existing owners for A02/A03/N03 together; this is not complete acceptance of Go privilege/server/config packages. Existing SQL TLS policy must never disappear because a different account property changes.

## Progress

- [x] Read repository instructions and refreshed requested branches; no incoming changes.
- [x] Add grouped fail-before policy reload/export and CA/partial-material regressions.
- [x] Establish runtime baseline; compile/fixture failures do not count.
- [x] Connect durable global_priv load/export/writeback and shared registry mutation/reload/drop/rename.
- [x] Pass inbound CA/min-version configuration through bootstrap and verified certificate state into authentication; maintain X509 admission.
- [x] Run grouped regressions, affected all-target checks, lint and locked build; self-review.
- [x] Update both registers and durable receipts; use the normal hook-gated local commit and verified recovery/startup workflow. Actual commit/hook/bundle/draft results are recorded in /workspace/.cloud-setup/account-tls-batch/final-handoff.json. No push.

## Context and Orientation

At the starting commit, rust/crates/tidb-exec/src/cluster_privilege_load.rs reads a single storage snapshot but explicitly omits mysql.global_priv. cluster_account_write.rs reconciles the exported account image through one transaction. rust/crates/tidb-server/src/cluster_privileges.rs turns that image into the shared session PrivilegeRegistry. The registry currently keeps only a local ssl_type field and export omits it. mysql_tls.rs creates a fixed ServerConfig with no client verification; node_config.rs does not carry inbound CA/min-version to that constructor. configured_user_store.rs authenticates using a transport admission token. mysql_connection.rs creates that token after TLS handshake and before plugin negotiation.

## Milestones and Plan of Work

First run all new cases together against old behavior. Then retain global_priv independently of mysql.user so unrelated operations preserve unknown fields and orphan policy rows. Move ssl_type to that shared durable owner rather than retaining a duplicate enum field. Reload, scratch image publication, drop and rename must use it. Reconcile Priv as stored text alongside existing account tables. A malformed/unsupported policy must deny authentication rather than become no requirement.

Next configure inbound TLS with optional verified client certificates using the captured ssl-ca and minimum protocol. Pass verified certificate presence from the completed rustls connection through the existing admission token; a mere TLS boolean cannot satisfy REQUIRE X509. Preserve plaintext compatibility helpers, Go partial-material fallback and explicit refusal of still-unimplemented specified issuer/subject/SAN policy. Do not invent certificate parsing or bypass trust verification.

Finally combine affected library tests and prior account regressions, check all targets, run make lint and cargo build --locked -p tidb-server. The actual precommit hook must repeat and pass that build. Source and receipts share one ordinary local commit. No source changes in native client are planned; no sync/push is needed.

## Concrete Steps

Activate source /workspace/.cloud-setup/env.sh in every shell. From /workspace/tidb/rust baseline cargo test --locked -p tidb-server --lib -- account_tls_batch --test-threads=1. Capture /workspace/.cloud-setup/account-tls-batch/baseline.log. Final grouped tests include server account/TLS, execution cluster-account writer/loader and session REQUIRE regression modules in compatible package targets. Run the full cluster_account_write_source integration target separately; run only tidb-server --test all -- mysql_tls_source for its aggregate. Do not request all aggregate targets for unrelated packages. Run cargo check --locked -p tidb-exec -p tidb-session -p tidb-server --all-targets. From root run make lint. Use normal git commit with core.hooksPath=hooks; never bypass hooks. Verify source hashes, clean worktrees, bundle recovery and saved draft readback.

## Validation and Acceptance

REQUIRE SSL must survive load and export, raw unknown policy and orphan rows must round-trip, CREATE/ALTER REQUIRE mutations must persist and drop/rename/reload must govern the same rows. Real TLS handshakes must admit no client certificate under optional verification, admit a trusted client, reject an untrusted client, and enforce TLSv1.3 when configured. Verified certificate state must satisfy X509 while plaintext and certificate-free TLS do not. Invalid CA material prevents TLS startup. Preserve account password/locking/reload controls. Full secure multi-node and complete specified-policy/reload lifecycle remain separate obligations.

## Idempotence and Recovery

Preserve current local history and existing verified recovery bundle until a replacement verifies. No reset, force push or competing source continuation. Fixtures use ephemeral loopback sockets and remove only their temporary materials. Check disk before builds; retain sources, logs, production executables and fingerprints; journal disposal of obsolete or already-validated test executables. Compilation/fixture failure requires correction before baseline evidence is recorded.

## Interfaces and Dependencies

LoadedGlobalPriv carries host/user/raw Priv text. ClusterPrivileges includes an independent global_priv image. PrivilegeRegistry owns that image with shared clone/publication semantics and derives admission from it. MysqlServerTls policy-aware construction captures CA/minimum protocol; ClientStream provides verified peer certificate presence after handshake. TransportAdmission carries that evidence from wire to authentication. Reuse existing rustls/rustls-pemfile/rcgen/serde_json dependencies.

## Surprises & Discoveries

The current local enum survives only within one process: account export and cluster startup omit mysql.global_priv entirely. Inbound TLS additionally ignores ssl-ca/tls-version already recognized by the common config loader. Old REQUIRE X509 refusal tests describe the removed transport limitation and must be corrected when real verification exists; useful negative certificate tests remain. Eight old SHOW GRANTS tests expected single-quoted accounts even though current Go stringutil.Escape and existing Rust use backticks; only those quote expectations and the adjacent example are corrected. The shared config key is tls-version, not min-tls-version. Durable row ordering is not a storage contract: the fixture compares sorted logical rows while preserving exact raw JSON. Three debug links exhausted disk; these build failures are recorded separately from runtime evidence. Go encoding/json controls additionally require later null fields to leave earlier values intact, and earlier known-field type errors to remain broken even after a valid duplicate.

## Decision Log

Decision: maintain durable policy and verified transport together across A02/A03/N03. Rationale: loading policy without its enforcement or adding verification without persistent policy leaves the same broken shared lifecycle. Date: 2026-10-04.

## Outcomes & Retrospective

Five independent baseline cases failed at runtime. Durable policy, verified transport and account handoff are implemented; 114 distinct Rust cases and 12 Go JSON controls pass; all-target checks, lint and explicit locked build pass. The actual hook remains mandatory for the local commit. A grouped debug build exhausted disk and its failed result is retained. Canonical account-host matching and Unix-versus-TLS admission are included after Go source review. Broad findings remain partial; no complete package or multi-node acceptance is claimed.
