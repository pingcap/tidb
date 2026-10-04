# Share runtime HTTP settings with SQL and live configuration

This living plan follows root PLANS.md. Starting point is 93059cf47d60b423712ffbc9988b130bfe79f80f on hparser-integration. Go master freshly fetched remains93a01d31f6da205ae4bf376825293903a6899fdb. Native master19a56cc is unchanged. No push or dry run is authorized.

## Purpose / Big Picture


HTTP /settings updates should reach the same process configuration and global SQL-variable owners as SQL. GET /settings, GET /config and SQL SHOW CONFIG must read current configuration instead of startup bytes. This connects N05 administration, N03 configuration consumers and I01 live metadata. Implement the existing-owner maintenance boundary; complete pkg/server/handler/tikvhandler, session, config and executor package artifacts remain unaccepted.

## Context and Work


Go SettingsHandler.ServeHTTP in pkg/server/handler/tikvhandler/tikv_handler.go applies form fields in fixed source order, preserving earlier effects if a later field fails. Global transaction switches go through internal sessions; process settings share existing owners. Original tests are TestPostSettings and TestGetSettings in pkg/server/handler/tests. Both Rust startup paths use http_status::StatusRoutes and currently capture settings_json. Replace that duplicate snapshot with current global-config reads and a factory-owned settings callback. Reuse the advanced internal-session pool for durable GLOBAL writes and shared GlobalSysvars for instance controls. Keep unsupported transaction-summary recorder controls explicitly unaccepted because no recorder exists; never report those mutations successful.

Read complete HTTP headers and form bodies before dispatch, preserving query/body precedence and first-value semantics. Cover fragmented and chunked HTTP input using existing TCP tests. Repair shared config update serialization and the check-mb4 configuration publication consumer. Remove the stale refusal-only settings test after replacing it with Go behavior and failure cases.

## Progress


- [x] Refresh sources, trace owners and begin grouped real-server before regressions.
- [x] Implement nine shared settings, both factory paths, live getters, body framing and error ownership.
- [x] 63 distinct Rust tests, affected all-target checking, lint and self-review pass.
- [x] Update both registers and compact validation receipt. Actual commit hook, after-wire, recovery and Cloud draft outcomes are recorded externally after the gate.

## Milestones and Validation


The first milestone captures failures using /workspace/.cloud-setup/runtime-settings-batch/wire.py before against the retained normal server. The second changes the whole settings owner and consumers together. The third builds affected tests together from rust after source /workspace/.cloud-setup/env.sh, runs selected tests with nonzero counts, runs cargo check --locked -p tidb-server -p tidb-session -p tidb-config --all-targets and root make lint. The actual executable hooks/pre-commit must run cd rust && cargo build --locked -p tidb-server on the normal commit. Then run the same wire script after against the newly built binary with PYTHONPATH=/workspace/.cloud-setup/python. Results and exact commands live outside the checkout in the batch evidence directory.

Acceptance: one HTTP form updates multiple controls, SQL and current configuration see them, GLOBAL variables persist through mysql.global_variables, malformed/range errors retain earlier effects and return400, body values precede query values, hidden live configuration remains handled by the shared retriever. Full distributed/TLS/package and performance acceptance are not implied.

## Surprises & Discoveries


The configuration HTTP server reads startup bytes even though SQL now retrieves its endpoint live. The existing check-mb4 resolved SQL setting does not publish back into canonical configuration. Config update_global clones before locking and can lose unrelated concurrent updates. Transaction-summary recorder controls lack a production owner and remain unsupported.

## Decision Log


2026-10-04: batch the connected settings lifecycle rather than individual controls. Preserve source field order and partial-update semantics; reuse existing durable SQL and process owners. No native or concurrent schema-sync edits.

## Outcomes & Retrospective


Nine controls, both storage factories and shared publication/readers are implemented. Before:12 HTTP/SQL failures and one lost-update failure. After:63 distinct Rust passes, including two existing HTTPS-policy checks; two pre-existing ignored parity obligations remain (plus a subprocess helper executed by its parent). N05 moves to partial; N03/I01 retain broader obligations. Full Go suites, distributed/TLS server lifecycle, exhaustive HTTP parser/malformed/non-UTF8 forms, transaction-summary recorder and performance remain unverified. Final hook and after-wire evidence: /workspace/.cloud-setup/runtime-settings-batch/final-handoff.json. No complete package claim.

## Recovery


Keep concurrent changes and exact remotes. Changes can be reverted from the starting commit individually; never reset the checkout. Preserve the unpublished bundle and all evidence before publishing a new Cloud draft. Fresh-task restoration remains unverified.

Validation notes: initial compile found two VarError values without Display; corrected error adaptation. A receipt parser falsely matched zero inside “30 passed”; raw runners all passed and exact distinct test names were verified. The first isolated config probe accidentally exercised the repaired code because of a relative-path mistake; its separate log remains, and the correctly restored baseline subsequently fails. Wire expectations were corrected to preserve Go AtomicBool text JSON and normalize SQL threshold typing before the authoritative before run. One inactive old test executable was regenerated to fit disk; pruning is journaled.
