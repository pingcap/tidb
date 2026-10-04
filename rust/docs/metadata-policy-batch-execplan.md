# Live metadata and shared visibility batch

## Purpose and context


Maintain I01, I02 and I03 together: a created sequence must appear in SEQUENCES with its current definition and account visibility; security enhanced mode (SEM) must hide cluster endpoints and timing data; local process rows must carry this node's status address, not a randomly selected peer. The existing catalog, privilege registry and process topology remain the authorities. This is existing-owner maintenance, not complete upstream package acceptance or remote fanout completion.

Cloud starts at 580d205b2fee771224d5750b85a0adf2b7856135 in /workspace/tidb, branch hparser-integration. Fresh Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb; native /workspace/client-rust master stays 19a56ccda1e128218cd33c69709038219aced9bc. Concurrent remote integration 0a61ca9b586add9697a10f9bf8264931e3281a95 is preserved unmerged. No pushes or dry runs.

## Progress


- [x] Refresh remote references and compare live metadata producers with Go.
- [x] Five grouped session regressions fail with unchanged production; real MySQL also reproduces missing sequence rows and wrong instance port.
- [x] Replace the empty sequence reader, obsolete 13-column schema, and shared cluster identity/visibility paths.
- [x] 57 grouped Rust tests pass, including all five new regressions and the three previously failing parser groups; affected all-target checking and lint pass. Two existing ignored obligations plus the parent-invoked SEM helper remain.
- [x] Prepare the normal gated local commit; actual locked-build hook and rebuilt-server probe outcomes are recorded in the external final handoff after this receipt is staged.
- [x] Update both registers, the batch map and durable validation receipt.
- [ ] Verify the local commit, recovery bundle and saved environment draft; exact post-gate results belong in /workspace/.cloud-setup/metadata-policy-batch/final-handoff.json.

## Work and milestones


First extend the existing sequence and server-info session suites. Compare pkg/executor/infoschema_reader.go setDataFromSequences, dataForTiDBClusterInfo and setDataForServersInfo, plus pkg/infoschema/cluster.go GetInstanceAddr. The current sequence reader is constant empty; cluster readers omit SEM redaction; cluster_instance_address performs remote discovery then chooses an arbitrary entry and SQL port.

Next make SEQUENCES consume the existing SequenceDef and allocator definition without allocating a sequence value. Reuse SchemaVisibility with Go's any-privilege mask. Make the three cluster metadata paths share one RESTRICTED_TABLES_ADMIN decision with active roles and nil-checker behavior. GetInstanceAddr must use local_server_info and join_host_port with status_port. Remove its remote read and swallowed error. Preserve dynamic server IDs in CLUSTER_INFO, while the cluster-instance SEM value is the DDL ID string, as Go specifies.

Finally validate the connected boundary once. Do not claim I03 remote fanout or other I01 missing providers are repaired. Keep existing meaningful ignored obligations and unrelated baseline failures visible.

## Validation and acceptance


Source /workspace/.cloud-setup/env.sh and run Cargo from /workspace/tidb/rust with CARGO_BUILD_JOBS=1. Run the sequence, server-info and isolated SEM tests together, including original parser/executor cases. The exact combined Cargo command is in parity/current-audit/metadata-policy-batch-validation.json. Verify new regressions fail before edits. Check tidb-session and tidb-server all targets together, run make lint from the repository root and git diff --check. A normal local commit must execute hooks/pre-commit and cd rust && cargo build --locked -p tidb-server. Real MySQL/unistore checks use the pre-change executable and the hook-produced executable. Logs belong in /workspace/.cloud-setup/metadata-policy-batch.

## Surprises & Discoveries


The sequence metadata schema was also stale (thirteen non-Go columns). ALTER SEQUENCE had an implemented parser behind an earlier catch-all branch; repairing dispatch is required to observe live definition changes.

Go uses the status port for cluster table INSTANCE, the SQL port for CLUSTER_INFO INSTANCE, and the textual DDL ID for cluster-table SEM redaction but the numeric server ID for CLUSTER_INFO. These are separate source contracts.

## Decision Log


- 2026-10-04: batch catalog metadata and shared cluster visibility, preserve concurrent schema work and existing ownership. Avoid introducing a second topology or catalog cache.

## Outcomes & Retrospective


Five initial regressions fail before repair. The first grouped run exposes an existing ALTER SEQUENCE dispatch defect: the generic ALTER error branch shadows its existing typed parser. An existing session test and three retained original-Go parser groups reproduce it. Move the sequence branch before the generic fallback, retaining both the existing parser and diagnostics for unsupported ALTER forms. The final grouped run passes57 tests (parser6, executor23, session26, server1, exec1); Domain selects zero and receives no validation credit. Affected all-target checking and make lint pass. Other54 unresolved findings retain historical evidence; this batch does not rerun the entire register.

## Recovery and interfaces


No dependency or native-client changes are required. Tests isolate process-global SEM in a subprocess. Keep disk use bounded by preserving current artifacts and pruning only identified superseded outputs. Preserve source, logs and the unpublished recovery bundle. Recreate and verify the bundle after a successful gated commit; saving the Cloud draft is not publication or fresh-task restoration.

Final preparation: self-review checks source schema/order/flags, no sequence allocation in metadata, absent versus attached privilege checker, active-role changes and distinct SQL/status/DDL identities. No dependencies, generated files, native client sources or concurrent schema-sync files change. The recovery bundle and Cloud draft are finalized after the actual locked-build hook and wire checks; no push is authorized.
