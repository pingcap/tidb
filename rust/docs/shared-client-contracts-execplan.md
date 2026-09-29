# Share native client contracts with TiDB

This living ExecPlan follows PLANS.md. Update Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective throughout the work.

## Purpose and acceptance


Use the same complete KV protocol and Variables contracts in TiDB and client-rust, as Go does. Preserve resource-control penalties and nested region metadata across requests. Detached DistSQL contexts must copy all variables and retain SQL kill identity. Explicit operation lifetimes and complete region/RPC ownership consolidation follow after accounting for the concurrent transaction restoration.

This is integration and retirement of existing native packages, not a new Go-package transcreation claim. Integrate each generated package as a whole and do not hand-edit generated outputs. Complete client package migrations require full inventories and validation before reporting parity.

## Context and source anchors


Go master is 12b639a1161cd5a60126a47277f5ad14c320fd4a, with client-go v2.0.8-0.20260928031501-8edb23f6c7ee and kvproto v0.0.0-20260820070758-623e58e60fa9. Sources are in /Users/qiliu/go/pkg/mod/github.com/. Native client-rust master is 8b890e2b0e1c40842f91dd865783431cf986a811 in /Users/qiliu/projects/client-rust.

The initial pull advanced TiDB from 3e3cdcf0f8 to 32b55c041c. The incoming commit restores previous transaction/resolver implementations, removes the reviewed bridge, and restores an older vendored client and patches. Preserve that incoming work pending user direction; protocol and Variables integration remains independent.

Go pkg/kv/variables.go aliases client-go kv.Variables. Go pkg/distsql/context copies the complete struct during detach, then points Killed at SQLKiller.Signal. The Go driver supplies PD and client-go RPC clients to KVStore and retains tracing/error adapters. Operation contexts differ: primary commit uses caller context, secondary commit and cleanup store context, and heartbeat background context. Cleanup clears query-kill callbacks while retaining its context and retry limit.

## Progress


- [x] Refresh both repositories and verify Go ownership boundaries.
- [x] Detect the concurrent transaction restoration and request direction.
- [x] Record failing protocol tests, integrate complete generated KV packages, migrate consumers.
- [x] Reuse native Variables in txnkv and DistSQL, including detach/kill tests.
- [x] Validate the independent integration; publish only through the locked server build gates below.
- [ ] Reconcile the transaction restoration before explicit operation-lifetime migration.
- [ ] Inventory and validate native routing/transport coverage, migrate callers, retire duplicate algorithms.

## Package inventory


The dependency also requires complete encryptionpb because metapb.Region carries EncryptionMeta; the local encryption projection would shadow that input. The complete input/output inventory and SHA-256 comparisons are in shared-client-contracts-inventory.json. The kvproto packages contain pkg/kvrpcpb/kvrpcpb.pb.go, pkg/kvrpcpb/shared_lock_lost_test.go, pkg/errorpb/errorpb.pb.go, and pkg/metapb/metapb.pb.go, and pkg/encryptionpb/encryptionpb.pb.go. There is no doc.go or platform-specific source in these directories. Full proto inputs/import closure are already vendored in rust/third_party/tikv-client-rs/proto; generated Rust and descriptors are in kvproto/src/generated; generation is owned by proto-build. Re-export whole packages and map them with prost extern_path; delete all four local projected inputs. Retain existing wire fixtures and cover the upstream shared-lock-lost test.

Client-go kv production sources are kv.go, key.go, keyflags.go, store_vars.go, variables.go; tests are main_test.go, kv_test.go, key_test.go. Native kv already owns these contracts. Variables consumer integration makes no whole-package parity claim. Preserve existing source-contract tests.

## Milestones and concrete steps


First extend tidb-proto/tests/transaction_wire_source.rs with native-to-TiDB-to-native round trips and run before production edits. Update tidb-proto/build.rs to use native proto imports with package extern_path mappings. Re-export kvrpcpb, errorpb, metapb and encryptionpb from the native generated crate, with aliases for former local peer/epoch names. Remove local redundant inputs. Update affected consumers for complete native fields using Go defaults and retain their existing behavior. Keep TiPB and opaque BatchCommands encoding separate.

Then replace both reduced Variables definitions with native Variables. SQL kill handles expose the shared atomic rather than copying signal values. Detach clones all variables and rebinds only the kill signal, preserving file-transaction options and kill-handler identity. Extend regression coverage before changing behavior.

## Validation and delivery


From rust/, run cargo test --locked -p tidb-proto, cargo test --locked -p tidb-distsql --lib, cargo test --locked -p tidb-txnkv --test all kv_package_source::, cargo test --locked -p tidb-pd-client, and cargo build --locked -p tidb-server. Add focused transport/embedded consumer checks when changed. Preserve red/green logs under /private/tmp and record final exact commands below. No Go/Bazel inputs change, so bazel_prepare and Go failpoint setup are inapplicable. At repository root run make lint and git diff --check. Commit through TERM=xterm git -c core.hooksPath=hooks commit, which must execute the locked server build; rerun cd rust && cargo build --locked -p tidb-server immediately before pushing hparser-integration. Native changes require independent validation/push before vendor sync.

## Surprises & Discoveries


The incoming 32b55c041c invalidates the reviewed production-bridge assumption. A user clarification is pending. Independent protocol and Variables integration proceeds. The apply_patch call stalled without changing files and was terminated; normal shell file tools are used instead. Native Context contains floating-point resource consumption, so types that contain it retain PartialEq but cannot derive Eq. The protobuf import closure also requires native encryptionpb to preserve RegionEncryptionMeta.

## Decision Log


- Integrate complete generated packages instead of extending partial schemas. This follows Go type ownership and removes recurring omission risk. 2026-09-29.
- Preserve Rust compatibility patches, TiPB, and TiDB SQL/PD adapters: their removal is not justified by the Go comparison. 2026-09-29.
- Preserve concurrent transaction restoration pending user direction; do not silently undo performance work. 2026-09-29.

## Outcomes & Retrospective


The two initial protocol regressions failed before replacement and now pass. A third regression reproduced dropped scanned-version counters in the coprocessor-local copy of kvrpcpb execution details; those copied messages and metapb peer/epoch copies now use the same native types. The Variables defaults regression failed (0 rather than 10) before aliasing; all five context tests then passed, including complete-field detach and callback/kill identity. The full DistSQL aggregate suite has exactly the same 20 failing tests on the changed tree and the incoming 32b55c041c baseline (236 passed, 20 failed, 2 ignored on each). The isolated baseline checkout has been archived after testing. No full parity or performance claim. Real-cluster faults and sysbench/TPC-C/TPC-H/YCSB measurements have not been run.

## Recovery


Protocol aliases, removed schemas and consumer changes form one atomic integration. Do not revert only one side. Preserve incoming unrelated work. Do not introduce another transaction engine as a workaround.

## Validation evidence


All Cargo commands below run from `rust/` unless explicitly noted. Before production changes, each of these commands failed its new regression: resource-control penalties and region encryption metadata were lost, coprocessor scan counters decoded as zero, and DistSQL variables used zero instead of Go's lock-fast default of 10.

    cargo test --locked -p tidb-proto --test transaction_wire_source shared_
    cargo test --locked -p tidb-proto --test transaction_wire_source coprocessor_details_preserve_native_scan_counters
    cargo test --locked -p tidb-distsql --lib test_context_uses_native_variable_defaults

After the fix, the following commands passed. They exercise the complete protocol crate, DistSQL variables/detach, KV aliases, PD wire consumers, lock/region recovery, and SQL transaction behavior. The required compile checks include all test targets for the affected production consumers.

    cargo test --locked -p tidb-proto
    # 34 passed
    cargo test --locked -p tidb-distsql --lib
    # 37 passed, 1 pre-existing ignored test
    cargo test --locked -p tidb-pd-client
    # 74 passed, 1 pre-existing ignored test
    cargo test --locked -p tidb-txnkv --test all kv_package_source::
    # 38 passed
    cargo test --locked -p tidb-txnkv --test lock_resolver_source --test region_error_recovery_source
    # 30 resolver and 26 region-recovery tests passed
    cargo test --locked -p tidb-txnkv --test all pd_region_loader_source::
    # 15 passed
    cargo test --locked -p tidb-txnkv --test all region_epoch_bucket_inheritance_source::
    # 2 passed
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::transactions
    # 25 passed
    cargo check --locked -p tidb-server -p tidb-txnkv -p tidb-distsql -p tidb-pd-client -p tidb-unistore --all-targets
    # passed

The broader DistSQL command was compared against the untouched incoming commit in a managed checkout. Both runs had 236 passed, 20 failed, and 2 ignored; the sets of failing test names were identical.

    cargo test --locked -p tidb-distsql --test all
    # changed tree: 236 passed, 20 failed, 2 ignored
    # baseline cwd: /Users/qiliu/.codex/worktrees/shared-contracts-baseline/tidb/rust
    CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target cargo test --locked -p tidb-distsql --test all
    # baseline 32b55c041c: identical failures and counts

The root `make lint` and `git diff --check` passed. The new baseline checkout's required `make bazel_prepare` was attempted and failed because `bazel` is not installed; Rust baseline tests ran successfully to completion. No Go/Bazel inputs changed. The one new Cargo.lock line records the native client as a DistSQL test dependency; `cargo test --offline -p tidb-distsql --lib test_context_` updated that dependency and passed five tests before subsequent locked validation.

Logs are `/private/tmp/shared-contracts-{protocol-red,variables-red,cop-details-red,protocol-green,distsql-lib,pd,kv,storage,server-transactions,consumer-build,distsql-all,distsql-baseline,lint}.log`. The complete inventory was checked against every file in the four pinned Go package directories; all recorded native generated outputs, build inputs and 11 imported proto inputs matched their SHA-256 records. No generated output was hand-edited.

The remaining delivery gates are mandatory and enforced separately: the commit must use the checked-in pre-commit hook, whose Rust step is `cd rust && cargo build --locked -p tidb-server`; immediately before pushing, rerun the same locked build. Use these commands from the repository root after staging only this integration. A successful commit/push is the receipt that the gates passed; do not bypass the hook or force-push over newer work.

    TERM=xterm git -c core.hooksPath=hooks commit -m "rust: share native KV protocol packages and variables"
    cd rust && cargo build --locked -p tidb-server
    git push origin HEAD:hparser-integration

Correctness and compatibility evidence covers full existing wire contracts and the affected consumers. Complete native types intentionally expose fields absent from the projections and drop invalid Eq derives where protobuf messages now contain floats. Existing absent-field defaults and wire numbers remain unchanged. Performance benchmarks, real TiKV faults, the broader transaction ownership migration and the 20 baseline DistSQL failures remain unverified or unresolved; this integration makes no claim to fix them.
