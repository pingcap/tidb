# Durable login and generated TLS configuration

## Purpose and context


Maintain the connected A02, A03 and N03 admission owners. Failed passwords, successful resets and expired automatic locks must update mysql.user through the shared storage transaction, survive FLUSH PRIVILEGES, and retain unrelated JSON. Automatic TLS must consume the canonical RSA size and temporary storage path, generate Go's certificate identity, and use the existing reload/rotation owner. Remove the in-memory-only production shortcuts after migrating their callers. These are existing-owner repairs, not complete Go package acceptance.

Work in /workspace/tidb on hparser-integration from 8d792a3297f9e6b0d513e6a34c0e473875e5ef47. Freshly fetched Go master is 93a01d31f6da205ae4bf376825293903a6899fdb in /workspace/.cloud-setup/go-master; client-rust master remains 19a56ccda1e128218cd33c69709038219aced9bc. Preserve concurrent remote integration 0a61ca9b586add9697a10f9bf8264931e3281a95 unmerged. No push or push dry run. Follow root AGENTS.md and PLANS.md.

## Progress


- [x] Refresh references and recheck the three selected findings against current source.
- [x] Reproduce durable counter and configured certificate failures with the existing executable.
- [x] Compose shared login policy, pessimistic storage transactions and post-commit publication.
- [x] Compose canonical generated TLS material and remove redundant resolution paths.
- [x] Run grouped regression and affected all-target checks, make lint and self-review.
- [x] Update registers and receipts; prepare the normal gated commit.
- [ ] Actual hook, final wire replay and recovery/draft reference verification: results are finalized in /workspace/.cloud-setup/auth-durability-batch/final-handoff.json after this source receipt is staged.

## Milestones and implementation


First record before-images using the existing executable and extend the existing account/unistore/TLS suites. Go references are pkg/session/session.go Auth, verifyAccountAutoLock, authFailedTracking and authSuccessClearCount; pkg/privilege/privileges supplies the JSON and cache contracts; pkg/util/misc.go LoadTLSCertificates and CreateCertificates supply certificate policy.

Next move the three locking transitions into a shared policy function consumed by both PrivilegeRegistry and the durable login path. Use ClusterSessionFactory's existing internal storage sessions, BEGIN PESSIMISTIC, SELECT FOR UPDATE, parameter escaping and commit/rollback. Only the locking JSON fields change. Failed storage cannot publish a successful admission or cache change. Both TiKV and unistore startup attach the same owner. Keep bypass and disabled tracking free of storage reads. Preserve Go's cache precheck and authoritative post-password check.

Then route automatic TLS through NodeConfig's canonical RSA size and resolved spill directory. Generate a 90-day RSA certificate with serial 1, Go's common name and OS hostname DNS SAN, write cert.pem and mode-0600 PKCS8 key.pem, and retain the existing certificate resolver and joined rotation worker. Configured pairs still take precedence, partial pairs follow auto-TLS, and SQL-visible configuration paths retain their configured values. Do not add another renewal loop.

## Validation and acceptance


Source /workspace/.cloud-setup/env.sh in each shell; Cargo runs from rust/ with CARGO_BUILD_JOBS=1. Logs live under /workspace/.cloud-setup/auth-durability-batch. Group the existing account, TLS and unistore regressions; verify the corresponding before-image failures. Test failure count/reset/lock/unlock, stale caches, concurrent attempts, unrelated attributes and transaction failure; test certificate identity, key size, permissions, publication, reload and invalid paths. Run affected all-target cargo check and root make lint once at the final boundary. The actual hooks/pre-commit must run cd rust && cargo build --locked -p tidb-server on the normal local commit. Reuse that executable for real MySQL/unistore checks. No whole-suite, live multi-node, performance or complete-package claim follows from targeted checks.

## Surprises & Discoveries


Current login mutates only the privilege cache. Current automatic TLS generates an ECDSA localhost certificate in memory although canonical configuration already contains RSA size and a resolved temporary directory. The existing internal SQL session path already owns pessimistic locking and rollback; reusing it avoids a second account-row transaction implementation.

## Decision Log


- 2026-10-04: batch A02/A03/N03 around admission and configuration; preserve shared policy and lifecycle owners and compile at the combined boundary.

## Outcomes & Retrospective


164 grouped Rust cases pass (20 server unit,26 server aggregate,118 session unit); the session aggregate selects zero and receives no coverage credit. Seven real-server assertions fail against the retained pre-change executable;18 pass after repair. Concurrent attempts, stale caches, expiry, missing durable rows, lock expiry, generated certificate identity/size/files/reload and existing TLS/account behavior are covered. All three findings stay partial for broader privilege cache, configuration and TLS platform obligations. Final check/hook/bundle/draft results are recorded below and in the external final-handoff.json.

## Recovery and dependencies


Preserve source changes, existing logs and the unpublished Git bundle. Prune only proven superseded regeneratable artifacts if disk pressure requires it. Certificate generation can reuse the already locked OpenSSL dependency for RSA generation and the existing rcgen/rustls owners. No native-client changes are planned. Recreate and verify the recovery bundle after the gated commit; saving a Cloud draft is not publication or a verified fresh-task restore.

The grouped build first exposed a missing Arc import and the old standalone harness's private source copies. Migrating that harness required replacing one private test constructor with the equivalent public empty-registry setup. Its19 behavioral cases now use the production library in the existing aggregate. No behavioral tests were deleted. The linker later hit disk exhaustion; the journal records only inactive superseded outputs removed, and the identical grouped command then passed. The first wire runner used the wrong temporary-path spelling; corrected tmp-storage-path before/after runs retain the same seven failures and then pass18 assertions. No source conclusion relies on that harness typo.

Self-review confirms one shared transition implementation, authoritative row locking, JSON merge preservation, post-commit publication, rollback and uncertain-commit error identity, expiry ordering, weak factory references and pool retirement. Canonical TLS resolver callers are all migrated; generated paths stay distinct from configured SQL-visible ssl_cert/ssl_key values. No native-client, generated Go/Bazel or concurrent schema-sync source changes occur. The OpenSSL edge reuses its existing locked revision; offline Cargo metadata updated that edge before failing to download an uncached Android-only dependency. Subsequent locked Cargo builds validate the lockfile. The current ring provider does not accept every RSA key size Go can generate (notably1024-bit keys); broader provider/platform parity stays explicit.

Affected tidb-util/tidb-session/tidb-server all-target checking passes in1m27s; root make lint exits0. git diff --check and register validation pass. The remaining external finalization records the normal hook and exact committed/recovery/draft identities without claiming an unpublished snapshot was restored in a fresh task.

The first normal commit was correctly aborted by its required hook: the cluster-session-smoke linker received SIGBUS after available disk fell to345MiB. Three already-replaced inactive test-profile executables were then pruned with their completed normal-build replacements retained. Retry the same normal commit; no hook is bypassed. Final outcome remains in the external handoff.
