# Connect timestamp settings to the shared historical-read owner

This living ExecPlan follows root PLANS.md.

## Purpose and scope


S04 and N03 share one missing execution boundary: tx_read_ts and tidb_external_ts are registered strings without their Go timestamp owners. Starting at 78196d5f4f549a5327506d25f773b8c9fb292c1b, follow refreshed Go master 93a01d31f6da205ae4bf376825293903a6899fdb. Implement one-use transaction timestamps, external timestamp storage hooks, and text/prepared historical reads together. Preserve the existing retained schema, row snapshot and active-timestamp guards. This is existing-owner repair, not complete package acceptance.

## Progress


- [x] Read instructions, live register and relevant Go session/variable/staleread/Domain owners; refresh master/integration refs without resetting local work.
- [x] Run ten real SQL baseline assertions together: seven fail, three pass.
- [x] Connect typed one-use state, SET validation/rollback, query/BEGIN precedence and retirement.
- [x] Connect external timestamp getters/setters to the existing process storage authority and shared SQL consumers.
- [x] Validate both stores' adapters, source-derived regression cases and real MySQL behavior in one grouped boundary; update both finding registers and receipts.
- [x] Run affected all-target checking, make lint and self-review. The normal commit containing this plan is gated by the actual precommit locked build; postcommit recovery evidence is retained in the Cloud handoff.

## Source and implementation map


Go variable/varsutil.go owns parsing and mutual exclusion; variable/session.go owns TxnReadTS use/cleanup; executor/set.go owns transaction gates, timestamp validation and retained schema. sessiontxn/staleread/processor.go selects explicit AS OF, one-use time, read staleness, then external time; explicit transactions ignore later switches. Domain/domain_sysvars.go delegates external timestamps to the storage oracle. Rust SessionVars, variables.rs, dispatch.rs and txn.rs must share those decisions. The server ClusterTransactions seam and its PD capability supply the same authority for TiKV and unistore. Do not introduce a separate SQL timestamp map or persist oracle state as the authority.

## Validation and recovery


Source /workspace/.cloud-setup/env.sh; run Cargo from rust/ with CARGO_BUILD_JOBS=1. Extend existing owning test modules, group related filters and run affected checks once after source changes settle. The retained real-server probe is /workspace/.cloud-setup/timestamp-entrypoints-batch/wire.py; run it with PYTHONPATH=/workspace/.cloud-setup/python and a phase plus target/debug/tidb-server path. Baseline receipt records all ten results. Repository lint and actual hooks/pre-commit must pass cargo build --locked -p tidb-server. Git write access remains denied; do not repeat publication without changed access evidence. Recover individual before-images from the starting commit, preserving other work.

## Surprises & Discoveries


The native client already implements external timestamp RPCs; the running TiDB adapter and SQL global hooks do not consume them. Keep one process authority and existing cancellation/transport ownership. One-use SET skips the explicit mysql.tidb GC query as Go does, while native reads retain GC visibility validation. Preserve meaningful tests and earlier failures; no cleanup-only finding closure.

## Outcomes & Retrospective


Eighteen distinct final Rust cases and all thirty wire assertions pass; twenty-three wire assertions failed on the unchanged baseline. Affected all-target checking, lint and locked build pass. S04/N03 retain their broader partial dispositions. Full Go suites, live multi-node behavior, complete package acceptance and performance gains are not claimed.

## Decision Log


Keep one historical schema/data provider for text, prepared and BEGIN selection. External timestamps reuse the existing PD worker and unistore atomic authority. Go's external RPCs inspect only response errors, so absent or different-cluster headers are not rejected by this method. Reads remain bounded and shutdown-aware, with no invented retry or command metric. The wider native lifecycle migration remains open.

The final source review includes pending-setting write/locking-read admission, external BEGIN selection and prepared point-get cache bypass. These share the same session guard and timestamp selectors. Rust-only discard checks are not added; regressions exercise Go source contracts in existing suites.

## Milestones and acceptance


First, replace inert timestamp strings with typed one-use state and live oracle hooks in tidb-session/vars.rs, variables.rs and tidb-server/cluster_session_node. Then route query/prepared/BEGIN selection, write admission and cleanup through existing owners in dispatch.rs, txn.rs and prepared_ast.rs. Finally exercise both external adapters and real SQL behavior together. The final baseline has 30 checks: 23 fail and seven pass on the unchanged server. Acceptance requires all matching after checks to pass; targeted Rust lifecycle and PD RPC tests must pass, followed by affected all-target checking, lint and the locked build.

Compilation corrections and the expanding source review are retained in /workspace/.cloud-setup/timestamp-entrypoints-batch. A wrong-directory invocation did not compile or execute tests. Initial compiler failures were undefined context/value names and a String argument mismatch; they were repaired rather than counted as test runs. The first server and PD runs each passed two cases; the initial transaction suite passed fourteen. Final source validation passed; actual commit hook and recovery are the remaining publication boundary.

The final compatible library run passed 16 cases; the PD target passed two. A prior overbroad --test all selection unnecessarily linked the server integration target and exhausted disk. Its failed linker temporary file also blocked the next attempt. Both failures are preserved; explicit removal of that temporary file and unselected reproducible executables restored space. No test source, assertion, lockfile or dependency was removed. Artifact hashes are in artifact-cleanup.json. Select --lib for server/session and --test all only for the PD package.

## Final validation and handoff


Final commands from rust/ (with the Cloud environment sourced and CARGO_BUILD_JOBS=1): cargo test --locked -p tidb-server -p tidb-session --lib -- timestamp_entrypoints_batch historical_read_batch tests_core::transactions --test-threads=1 (16 passed); cargo test --locked -p tidb-pd-client --test all -- timestamp_entrypoints_batch global_config_preserves --test-threads=1 (two passed); cargo check --locked -p tidb-server -p tidb-session -p tidb-pd-client -p tidb-txnkv -p tidb-unistore --all-targets; cargo build --locked -p tidb-server. Root make lint passes. The final wire probe passes all thirty checks against the built unistore server. See parity/current-audit/timestamp-entrypoints-batch-validation.json for source identities, exact before/after results and limitations.

Both finding registers now replace the stale entrypoint allegation with the implemented behavior. No whole-package finding closes: SafeTS topology, ordinary-transaction provider switching, full transaction/prepared matrices and other configuration consumers remain partial. The other 54 unresolved roots were not freshly revalidated. No native source changes, benchmark gain or live multi-node acceptance is claimed.

The coordinator's 2026-10-05 read-only access investigation confirms PingCAP App installation 90274244 excludes pingcap/tidb. Account metadata reports admin/push, but OAuth reconnect did not add the repository grant. No permission changes or new push occurred. Correct the installation grant, then verify Cloud Git writes after the mandatory fresh locked build; retain the exact destination and never force-push. Preserve the updated recovery bundle and saved startup draft; neither proves fresh-task restoration or environment publication.
