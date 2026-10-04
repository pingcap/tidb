# Share account policy and live TLS configuration


This living ExecPlan follows PLANS.md. The user requests coherent multi-finding batches following current Go and grouped validation; no push is authorized.

## Purpose and context


GRANT REQUIRE must update the same durable policy as CREATE/ALTER USER. New MySQL handshakes must reload configured certificate files, retaining the last valid key pair on a bad replacement. ALTER INSTANCE RELOAD TLS must publish a new shared configuration with Go's rollback/no-rollback and secure-transport policy. Client-certificate requests without a CA must never turn an unverified certificate into X509 account evidence. Generated-certificate renewal must share this process owner and retire cleanly.

Start at Cloud /workspace/tidb hparser-integration b50ef5254ea601e04b07b300246b1d3acca1d6ea; native /workspace/client-rust master19a56ccda1e128218cd33c69709038219aced9bc is unchanged. Fresh Go master93a01d31f6da205ae4bf376825293903a6899fdb is separately exported in /workspace/.cloud-setup/go-master. Concurrent remote integration91010d96bb3e32468bd56ea2b07d255248485489 stays preserved unmerged. A02/A03/N03 maintenance does not accept complete privilege/server/util packages.

## Progress


- [x] Refresh sources, inspect Go server/util TLS and executor GRANT/ALTER INSTANCE ownership plus current Rust callers.
- [x] Capture grouped original-source regressions using existing server binary and existing TLS/session test owners.
- [x] Compose shared account policy, live certificate resolver/config reload, process retirement and session dispatch; migrate all production callers.
- [x] Run grouped relevant tests, affected checks, make lint and self-review.
- [x] Update both registers/receipts and pass the six-case real-server probe before commit.
- [ ] Final boundary: actual normal-commit locked server build, final rebuilt-server wire replay, recovery bundle and startup draft. Exact outcomes are recorded externally after they occur; no push.

## Plan of work and milestones


Extend existing account.rs tls_policy_of consumers to GRANT, after privilege/target validation and before publication. Existing account staging and typed global_priv writer preserve atomicity/durability.

In mysql_tls.rs retain configured certificate/key paths in a rustls certificate resolver. Read a valid pair at each handshake as Go LoadTLSCertificates.GetCertificate does; preserve the previous pair on errors. Own the current TLS configuration once per ConcurrentSqlNode, read it for each connection, expose a reload operation through the same process/session manager, and retire automatic renewal with that process. Only validated CA chains may reach verified account peer evidence; request-only certificate parsing remains untrusted.

Classify ALTER INSTANCE as process state, authorize SUPER as Go planbuilder does, and invoke the shared reload owner. Preserve active connections when replacing the config. Ordinary reload failure leaves it unchanged; NO ROLLBACK ON ERROR can disable future TLS only when require_secure_transport is false. Configured material startup failure remains an error.

## Validation and acceptance


Activate source /workspace/.cloud-setup/env.sh; run Cargo in rust/ with --locked and CARGO_BUILD_JOBS=1 for heavy links. Use existing session grant tests and server TLS/socket owners rather than creating a new mock server. Keep baseline/final logs under /workspace/.cloud-setup/tls-owner-batch. Group filters per compatible target. Required final gates: affected all-target cargo check, root make lint, git diff --check, and the actual hooks/pre-commit command cd rust && cargo build --locked -p tidb-server on a normal local commit. No redundant preliminary server build.

Observable checks: GRANT TLS policy persists and invalid/missing users leave all targets unchanged; new handshakes see valid file replacement while bad files retain the prior pair; reload succeeds, rolls back errors, handles no-rollback, and rejects insufficient privilege; unverified requested certificates cannot satisfy REQUIRE X509; generated renewal shares publication and joined retirement. Real MySQL/unistore checks use the rebuilt server after the hook. No full Go suite, live multi-node or performance claim is implied.

## Surprises & Discoveries


Go reloads configured cert/key files on every handshake, independently of ALTER INSTANCE. Its no-CA RequestClientCert is selected only when require_secure_transport is true. The starting Rust server had a static config and assumed every supplied peer certificate was verified; the replacement explicitly retains verification provenance.

A grouped integration-target build also links Cargo binary targets. It exhausted the32GiB filesystem while linking select-one-profile. A journaled cache-only removal reclaimed4847MiB from49 superseded archives/failed temporary outputs; no source, logs, fingerprints or recovery files were removed. An invocation from the repository root was stopped because Cargo needs rust/ as its current directory to select the maintained flags. These aborted attempts are not passing validations.

## Decision Log


- Decision: maintain A02/A03/N03 together at the admission/configuration owner boundary, using retained policy and process/session lifecycles. Rationale: these consumers share policy publication and TLS evidence; independent patches would repeatedly rebuild the same crates. Date: 2026-10-04.
- Decision: preserve all concurrent work and no-push; native dependencies unchanged. Rationale: publication is not needed for this owner batch. Date: 2026-10-04.

## Outcomes & Retrospective


The account/configuration/connection/process owners are composed. Original real-server probes fail all four grouped contracts. The first grouped Rust run passes150 tests; the added durable SQL reload regression brings distinct passing coverage to151. Its moved-value and CLI-fixture authoring errors were corrected, with all26 server library cases passing afterward. Seven TLS integration cases and118 session/grant cases retain the completed grouped results. The session all aggregate selects zero cases and is not coverage. The corrected six-case real MySQL/unistore run passes, including durable global_priv/FLUSH, certificate refresh/fallback, reload rollback/downgrade/recovery, existing TLS connection survival and SUPER1227. Affected all-target checking, formatting, make lint and self-review pass. The actual normal hook/final replay/recovery/draft outcomes belong in the external final-handoff.json after completion.

Counts remain86 tracked,29 repaired,57 unresolved(39 open,18 partial). A02/A03/N03 stay partial. Durable login counters and complete privilege-cache owners remain. Generated RSA sizing, hostname/CN/serial/temp-file publication and broader TLS platform/algorithm/parsing obligations are not repaired. Other54 unresolved IDs retain earlier evidence; no whole-package, full Go suite, multi-node or performance acceptance is claimed.

## Recovery


Revert only owned hunks if necessary; never reset/force-push. Preserve the verified unpublished bundle. After a gated normal commit, refresh it, survey complete workspace repositories and save exact local refs/startup instructions without changing other Cloud settings. Actual SHA-dependent outcomes belong in external final-handoff.json. Draft save does not publish or prove fresh-task restoration.

## Concrete commands and evidence


From /workspace/tidb/rust, after activating env.sh, run CARGO_BUILD_JOBS=1 cargo test --locked -p tidb-session -p tidb-server --lib --test all -- tests_grants mysql_tls tls_owner configured_user_store:: sql_node::tests --test-threads=1. Read every target summary; the session aggregate all selects zero cases, which is not coverage. Run cargo check --locked -p tidb-session -p tidb-server --all-targets and root make lint at the same batch boundary. The external wire-regressions.py uses the actual server and existing PyMySQL client; baseline0pass/4fail identifies original gaps. Final wire checks also require durable mysql.global_priv/FLUSH, SUPER1227, secure no-rollback refusal and existing-connection survival.

Revision note: composed the account, connection and process owners; added explicit certificate trust provenance and corrected the Go capability probe expectation. Kept generated RSA/identity/temp-file and wider package obligations open.
