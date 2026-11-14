# Go extension execution port


This ExecPlan follows root `PLANS.md`. Go `pkg/extension`, `pkg/extension/extensionimpl/bootstrap.go`, `pkg/server/extension.go`, and `pkg/privilege/privileges/privileges.go` own the contracts. Go source was read at `84261b5174fdb54ba5eb2929c335810ed3b10186` and was never modified.

## Purpose / Big Picture


Registered Rust extensions now authenticate and authorize connections, observe connection and SQL events, and execute bootstrap SQL through the domain's system session pool. Local TCP and embedded-store tests port Go's extension tests. The implementation is committed in `027877413fb`. Final validation is blocked by the Cargo environment; this report does not claim every check passed.

## Progress


- [x] Read root guidance and Go owners; preserve unrelated work.
- [x] Port executable authentication, credential callbacks, and post-grant authorization.
- [x] Port ConnectionInfo, handlers, lifecycle events, statement events, and FunctionContext metadata.
- [x] Port bootstrap contracts, SQL execution, record-set draining, and pooled-session return.
- [x] Capture Go auth-edge output and verify regression sensitivity by disabling production wiring.
- [x] Run the nine changed crates' suites and a server suite after the connection-field correction.
- [x] Compare all failures against unchanged source and commit the implementation.
- [ ] Compile the final native Digest return-type adjustment and rerun the seven cases that passed on unchanged source against the implementation. Cargo is blocked before compilation.

## Surprises & Discoveries


Go merges auth plugins globally but replaces the per-session auth map for each manifest with a non-nil plugin list. The port preserves that distinction. Authorization callbacks remain bound to the authenticated session and run only after SQL grants permit the operation.

Go's auth tests exposed the password-reuse bypass for extension plugins and the need to evaluate SYSTEM_USER and RESTRICTED_USER_ADMIN independently before the account loop. The statement-event tests exposed missing INSERT targets, PREPARE leaking its inner tables, retained parameters on a missing EXECUTE, and the binary prepared schema-change error wrapper. These were corrected at their owning paths, not hidden in the event adapter.

Go's connection fields use the configured server port, including Unix connections, and PeerHost supplies localhost for Unix sockets. The final server suite covers that correction. SQLDigest must return the existing parser Digest object; the final representation adjustment is implemented but has not completed compiler validation.

## Decision Log


Use the task's explicit six-worker setting and all four required Cargo environment variables instead of the general twelve-worker preference. Keep incremental compilation disabled for every check and test. Never delete shared target artifacts. Date: 2026-11-14.

Use native callbacks in `tidb-extension`, the existing session/privilege owners for execution, and the domain's advanced system session pool for bootstrap. Only copy statement facts when an event listener is installed. Date: 2026-11-14.

Keep X01 partial and retain its counts: the SQL/PB signature and alternative-plan portions are outside this task. Also record the outstanding final validation explicitly. Date: 2026-11-14.

## Outcomes & Retrospective


The Go-pinned auth, function-context, event, bootstrap, and existing registry tests passed. The complete nine-crate run was **7,129 run: 7,120 passed, 9 failed, 7 skipped**, taking 95m18s. It preceded the final connection-field and Digest API adjustments. A subsequent server suite covered the connection-field correction: **480 run: 477 passed, 3 failed, 2 skipped**. All extension tests passed there, including the Unix field test.

Three failures reproduced on unchanged `84261b5174fdb54ba5eb2929c335810ed3b10186` at the same assertions: `fetches_server_info_and_merges_logs_over_grpc`, `debug_pprof_profile_serves_a_gzipped_cpu_profile`, and `status_port_serves_diagnostics_to_cluster_tables`. They are baseline failures.

The other six failures from the broad run, plus `debug_zip_serves_the_profile_config_and_version` from the server rerun, all passed together on unchanged source (7/7, 18.13s). Their implementation rerun has not executed, so they are not declared resolved or attributed to the environment merely from their names. The cases cover two timestamp-age assertions, SLEEP's 200ms bound, statistics loading, partition statistics health, auto-analyze process ID, and the profile archive.

The latest successful nine-crate consumer check preceded the final Digest API alignment. Its first subsequent check failed writing a shared `tidb-variable/.../fingerprint/invoked.timestamp` file with OS error 2. Normal, offline, and isolated-output retries then stalled before compilation. Metadata-only Cargo queries and direct rustc version/target probes succeeded. The task did not delete shared artifacts or try to repair other processes. Tool-owned stalled retries were stopped. Final compilation and implementation reruns remain open.

No live PD/TiKV/etcd deployment or ignored integration-corpus replay was performed. Local TCP and embedded-store results are not a live-cluster claim.

## Context and Orientation


`rust/crates/tidb-extension/src/auth.rs`, `session.rs`, and `bootstrap.rs` define Go's contracts. Manifest options and Extensions dispatch them. `tidb-privilege/src/password.rs` and `user_privileges.rs` supply credential and post-grant checks; `tidb-session/src/domain/configured_user_store.rs` executes authentication.

`tidb-server/src/mysql_connection.rs` supplies auth packets, connection lifecycle, query, COM_INIT_DB and binary execution hooks. `tidb-session/src/extension.rs` reads retained statement facts from `observation.rs`. `tidb-variable/src/connection_info.rs` supplies ConnectionInfo, including the FunctionContext path. `tidb-session/src/extensionimpl/bootstrap.rs` adapts the domain's pooled system sessions.

## Plan of Work


Implementation is complete in source. Restore a functioning Cargo build environment without deleting another worker's artifacts. Run the final consumer check below, then the final selected-suite command. It runs all server and extension-crate tests, all extension session tests and bootstrap, and the seven outstanding comparisons. Inspect every failure; do not weaken expectations or assume an unchanged-tip pass proves the implementation passes. Update this report and the X01 validation clause only when evidence permits it.

## Concrete Steps


All Cargo checks/tests were run from `rust/` with these settings; nextest also received CARGO_BUILD_JOBS=6:

    export CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never
    export CARGO_PROFILE_DEV_DEBUG=0
    export CARGO_PROFILE_TEST_DEBUG=0
    export CARGO_INCREMENTAL=0
    export CARGO_BUILD_JOBS=6

Successful consumer checks before the final Digest API adjustment included:

    cargo check -j6 -p tidb-server -p tidb-session -p tidb-extension --tests
    cargo check -j6 -p tidb-variable -p tidb-exprctx -p tidb-sessionctx -p tidb-expr -p tidb-executor -p tidb-extension -p tidb-privilege -p tidb-session -p tidb-server --tests

Complete suite commands executed:

    cargo nextest run -j6 --no-fail-fast -p tidb-variable -p tidb-exprctx -p tidb-sessionctx -p tidb-expr -p tidb-executor -p tidb-extension -p tidb-privilege -p tidb-session -p tidb-server
    cargo nextest run -j6 --no-fail-fast -p tidb-server

Targeted extension validation included:

    cargo nextest run -j6 --no-fail-fast --lib --test all -p tidb-session -p tidb-server -E 'test(test_extension_stmt_events) | test(tests_extension_auth) | test(test_extension_conn_event) | test(test_auth_plugin_tcp_exchange) | test(tls_owner_request_without_ca)'

Unchanged-tip comparison commands executed (source snapshots restored in finally blocks, no branch/worktree switch):

    cargo check -j6 -p tidb-executor -p tidb-server --tests
    cargo nextest run -j6 --no-fail-fast --lib -p tidb-executor -p tidb-server -E 'test(fetches_server_info_and_merges_logs_over_grpc) | test(debug_pprof_profile_serves_a_gzipped_cpu_profile) | test(status_port_serves_diagnostics_to_cluster_tables)'
    cargo check -j6 -p tidb-session -p tidb-server --tests
    cargo nextest run -j6 --no-fail-fast --lib -p tidb-session -p tidb-server -E 'test(=domain::cluster_session_node::tests::unistore_cop::modify_column_runs_as_a_persisted_job_like_go) | test(=domain::cluster_session_node::tests::unistore_cop::mpp_execution::test_vector_search_heavy_function) | test(=domain::cluster_session_node::tests::unistore_cop::partition_global_stats_health_matches_go) | test(=domain::cluster_session_node::tests::unistore_cop::production_auto_analyze_is_killable_by_its_process_id) | test(=http_status::schema_route_tests::debug_zip_serves_the_profile_config_and_version) | test(=tests_core::sleep::sleep_uses_statement_conversion_and_waiting_semantics) | test(=tests_modify_column_null::a_null_becomes_the_current_timestamp_when_the_column_turns_not_null)'

The Go capture was run from the repository root and passed. The temporary probe was removed; only its output fixture is committed. It had no failpoint references and required no instrumentation:

    GOMAXPROCS=6 go test --tags=intest -p 6 ./zzprobe/x01_extension -run TestX01ExtensionAuthEdges -count=1 -v

The final consumer command was attempted but did not complete after the shared metadata error. Narrow offline and isolated-output variants also did not reach compilation:

    cargo check -j6 -p tidb-variable -p tidb-exprctx -p tidb-sessionctx -p tidb-expr -p tidb-executor -p tidb-extension -p tidb-privilege -p tidb-session -p tidb-server --tests
    cargo check --locked --offline -j6 -p tidb-extension --tests
    CARGO_TARGET_DIR=/private/tmp/x01-extension-validation cargo check --locked --offline -j6 -p tidb-extension -p tidb-session -p tidb-server --tests

The final selected-suite command below is **pending**, not a passed result:

    cargo nextest run -j6 --no-fail-fast -p tidb-extension -p tidb-session -p tidb-server -E 'package(=tidb-extension) | package(=tidb-server) | test(tests_extension_) | test(=extensionimpl::bootstrap::tests::test_bootstrap) | test(=domain::cluster_session_node::tests::unistore_cop::modify_column_runs_as_a_persisted_job_like_go) | test(=domain::cluster_session_node::tests::unistore_cop::mpp_execution::test_vector_search_heavy_function) | test(=domain::cluster_session_node::tests::unistore_cop::partition_global_stats_health_matches_go) | test(=domain::cluster_session_node::tests::unistore_cop::production_auto_analyze_is_killable_by_its_process_id) | test(=http_status::schema_route_tests::debug_zip_serves_the_profile_config_and_version) | test(=tests_core::sleep::sleep_uses_statement_conversion_and_waiting_semantics) | test(=tests_modify_column_null::a_null_becomes_the_current_timestamp_when_the_column_turns_not_null)'

## Validation and Acceptance


Go pins include `TestAuthPluginValidation`, every test in `auth_test.go`, `TestExtensionStmtEvents`, `TestExtensionConnEvent`, `TestExtensionFuncCtx`, and `TestBootstrap`. The TCP tests verify the actual auth-more-data exchange and packet sequence, refusal without callback diagnostics leaking, lifecycle/reset behavior, parse and execution errors, prepared parameters/digests/tables, and COM_INIT_DB events. TLS tests verify peer-certificate forwarding, with the additional Go output fixture below.

The negative witness disabled custom-plugin admission, statement callback fanout, and embedded-domain bootstrap. It compiled, then exactly three tests failed: auth returned plugin-not-loaded 1524, bootstrap's table was absent (1146), and the event test observed zero callbacks. All three source files were restored byte-for-byte:

    cargo check -j6 -p tidb-server -p tidb-session --tests
    cargo nextest run -j6 --no-fail-fast --lib --test all -p tidb-session -p tidb-server -E 'test(test_bootstrap) | test(test_extension_stmt_events) | test(=tests_extension_auth::test_auth_plugin)'

Acceptance still requires the pending final consumer/selected-suite commands. Preserve the known baseline failures and report their status separately from newly reproducible failures.

## Idempotence and Recovery


The worktree was initially clean. All implementation changes are under rust; no Go production files changed. Tests use registry reset guards. Baseline and negative-witness scripts saved and restored exact bytes. No other worktree was modified, no branch was switched, and nothing was pushed. No shared target artifact was manually deleted. Preserve the implementation and rerun checks after the Cargo environment recovers.

## Artifacts and Notes


`tidb-session/testdata/extension_auth_edges_go.txt` exactly matched the Go probe output with cmp. It pins plugin-only empty-hash validation, invalid-hash rejection, and TLS version/cipher/peer DER forwarding. Source fixtures use synthetic credentials.

Local logs, retained under /tmp, include `x01-full-tests.log`, `x01-final-server-tests.log`, `x01-negative-tests.log`, `x01-baseline-tests.log`, `x01-baseline2-tests.log`, `x01-final-api-check-interrupted.log`, and the stalled final check logs. The Go probe source was retained only in `/tmp/x01-extension-probe.go`, not in Git.

Both structural audit files retain **13 unresolved: 3 open, 10 partial**, with matching X01 text. The extension implementation and its precise validation boundary replace the old missing-implementation clause; unrelated X01 content remains unchanged.

## Interfaces and Dependencies


Go AuthPlugin, SessionHandler, SessionExtensions, BootstrapContext and their manifest options live in tidb-extension. Rust traits represent packet I/O, statement event information, and pooled bootstrap sessions. ConnectionInfo lives in tidb-variable. Privilege callbacks are bound after authentication; definer/other-user checks do not inherit them. Bootstrap drains record sets and returns its pooled session on exit. Authentication callback errors are sanitized before wire errors.

Revision 2026-11-14: recorded the implementation commit, complete suite results, both unchanged-tip comparisons, negative witnesses, and the external Cargo blocker. Final validation remains explicitly incomplete.
