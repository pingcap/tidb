# Share process administration state and policy

This living ExecPlan follows root `PLANS.md`.

## Purpose / Big Picture


KILL must use Go's live configuration and privilege gates, and PROCESSLIST must describe the inspected connection. This connected maintenance batch advances N04, A02, I01 and N03 together. It removes the duplicate connection-ID decoder and fabricated process statistics. It does not accept complete upstream packages or implement remote KILL before cluster identity exists.

## Context and Orientation


The Cloud checkout is `/workspace/tidb`, branch `hparser-integration`, starting at 955859e5860028f125ef6a3250ff7cc1b03fd335. Refreshed Go master is 93a01d31f6da205ae4bf376825293903a6899fdb, exported at `/workspace/.cloud-setup/go-master`. Native client master stays unchanged. Preserve remote integration work and do not push or run push dry runs.

`rust/crates/tidb-session/src/process.rs` owns connection registrations, trackers and statement metadata. `process_arm.rs` consumes these for SQL administration. `identity.rs` and the privilege registry own active/default roles. Go reference owners are `pkg/executor/simple.go::executeKillStmt`, `pkg/planner/core/planbuilder.go`'s KillStmt branch and restricted-user helper, and `pkg/session/sessmgr/processinfo.go::ToRow`. Security Enhanced Mode (SEM) requires explicit restricted privileges even for SUPER. A timestamp oracle value (TSO) packs milliseconds in its high bits and logical ordering in its low 18 bits.

## Progress


- [x] Confirm clean checkout and safely fetch Go master and integration; inspect live producers and consumers.
- [x] Capture one grouped fail-before regression run: seven independent failures.
- [x] Implement live KILL config, ordered authorization, restricted-account protection and target-owned process projection together; 158 grouped Rust cases pass.
- [x] Run grouped regressions (158 passed), SQL integration (8 passed), affected all-target checks, lint and locked build. Actual commit-hook completion is recorded in the final Cloud handoff.
- [x] Update both finding registers and reconcile all 57 unresolved batch assignments. The local commit, recovery bundle and draft finalization use the concrete sequence below; exact results live in the final Cloud handoff.

## Plan of Work


Extend existing tests in `tests_grants/processlist.rs`, isolating process-global policy mutations in child test processes. Reuse `tidb_util::globalconn::parse_conn_id` instead of the hand-written decoder. Authorize known local target users before applying execution-time config and parsing gates, with caller active roles and target default roles. Preserve the CONNECTION_ID() local path. Snapshot published digest, memory/disk consumption, TSO, resource group, alias and affected rows under the existing ProcessEntry lock. Format transaction time in the observer's session time zone. Keep unimplemented arbitration/CPU/reference-count lifecycles explicit.

## Milestones


First, independently failing regressions establish the connected policy and projection defects. Second, the shared registry and policy paths repair all selected contracts without parallel state owners. Third, a grouped validation boundary checks affected sessions and real MySQL connections, records remaining gaps, and creates a local commit through the mandatory hook.

## Concrete Steps


Activate `/workspace/.cloud-setup/env.sh`; use CARGO_BUILD_JOBS=1 and existing build outputs. From `rust/` run `cargo test --locked -p tidb-session --lib process_admin_batch -- --test-threads=1` before production edits, then run the affected process/privilege groups together after implementation. Run `cargo check --locked -p tidb-session -p tidb-server --all-targets`. From the repository root run `make lint`. Commit normally with TERM=xterm and `core.hooksPath=hooks`; the actual hook must run `cd rust && cargo build --locked -p tidb-server`. Store detailed logs under `/workspace/.cloud-setup/process-admin-batch`.

## Validation and Acceptance


Disabled global kill respects compatible-kill-query and KILL TIDB. CONNECTION_ID() bypasses numeric parsing. No session manager yields no parse warning. Authorization precedes execution warnings. SEM prevents an ordinary connection administrator from killing a restricted user, honors default/active roles and same-username exemption. PROCESSLIST uses the target's digest, tracker values, transaction time, group, alias and affected rows; timezone and no-context cases are checked. Real SQL integration must exercise accessible contracts. Whole Go suites, remote routing, live multi-node TiKV and performance remain unverified.

## Idempotence and Recovery


All checks are repeatable. Keep process-global test changes isolated, clean up only servers started by this batch, and preserve concurrent files. Do not broadly clean Cargo caches. If disk requires pruning, record only superseded regenerable outputs. Update recovery artifacts and draft only after a validated local commit; fresh-task restoration remains a separate unverified gate.

## Surprises & Discoveries


Seven baseline regressions failed independently and four of eight real MySQL assertions failed. The first wire fixture reached an existing CREATE RESOURCE GROUP refusal, so target-group semantics are covered in the Rust registry test. A second fixture assumed odd wire IDs, but the server allocates encoded even IDs; malformed/odd IDs remain covered by the Rust cases. Both unsuccessful wire attempts are retained in the validation receipt.

The registry already holds the seven projected values; the SQL surface discarded them. KILL's comments incorrectly claim SEM is unavailable, although the shared policy is active. Remote KILL still lacks the cluster-identity owner and is not repaired by local decoding.

## Decision Log


- Decision: repair connected local contracts without closing N04 or any package. Rationale: remote routing and complete package lifecycles are independent outstanding obligations. Date: 2026-10-04.
- Decision: reuse canonical config, roles and connection-ID parser. Rationale: duplicated execution policy caused the observed drift. Date: 2026-10-04.

## Outcomes & Retrospective


Seven connected contracts are repaired across N04/A02/I01/N03. All 158 grouped Rust cases and eight real MySQL/unistore assertions pass, with affected all-target checks, make lint, formatting and locked server build. Both registers and the 57-ID structural assignment agree: 29 repaired, 57 unresolved (34 open, 23 partial). N04 is partial because remote routing and internal auto-analyze authorization remain open; A02/I01/N03 retain their broader owner gaps. No complete finding/package or measured performance gain is claimed. Source/log hashes and exact commands are in `parity/current-audit/process-administration-batch-validation.json`. The actual hook, clean commit, bundle and draft outcomes are in `/workspace/.cloud-setup/process-admin-batch/final-handoff.json`; a draft save does not prove publication or fresh-task restoration. No push.

## Interfaces and Dependencies


Extend `process::ProcessRow` with values copied from its existing ProcessEntry owner. Keep `ProcessRegistry`, `Session::kill_stmt`, and `Session::process_list_table_rows` as the existing ownership and consumer boundaries. No dependency, lockfile or native-client changes are expected.

## Finalization


Commit normally as `session: share Go KILL policy and process-list state` with the activated toolchain, CARGO_BUILD_JOBS=1 and TERM=xterm. Retain the actual hooks/pre-commit log. Once the tree is clean, verify and replace the unpublished Git bundle, run the complete workspace repository survey, save the exact local HEAD and refreshed startup instructions, and read the draft back. Preserve installer/network/credential bindings. Stop on any failed hook or unsafe concurrent change; never bypass a gate or push. The external final handoff records these post-commit facts without a self-referential commit hash.

Revision note (2026-10-04): replaced pending implementation with the seven-contract repair and actual 158 Rust/eight SQL results; retained all broader package and lifecycle limits.
