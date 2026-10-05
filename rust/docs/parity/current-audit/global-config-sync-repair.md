# Global configuration synchronization repair

Historical configured TopN results below refer to the dated baseline. The now-unused
model and its private harness were retired by [unused-owner cleanup](leaf-owner-cleanup-validation.json);
these commands are not current verification instructions. Live executor tests remain.


This repair follows TiDB master
`93a01d31f6da205ae4bf376825293903a6899fdb` and starts from integration
`021de80c8cb40e8c63bbd17f273f13fefcf28354`, fast-forwarded before publication to
`1e570fd1e7f2f3ad606c415ff5b9c722e7573fae`. O12's missing explicit
SQL-to-PD publication owner is implemented. This does not repair O11's
profiling pipeline or accept the complete parent Domain/session/PD packages.

## Complete package boundary

The atomic source package is **pkg/domain/globalconfigsync**. Its complete
three-file inventory, Git blob IDs, original-test mapping, build mapping and
integration decision are in [global-config-sync-package.json](global-config-sync-package.json).
`globalconfig.go` maps to `tidb-domain/src/globalconfigsync.rs`.
`globalconfig_test.go` maps to the domain queue/store and server keeper tests;
the actual original Go tests, including TestMain's goleak harness, also ran.
`BUILD.bazel`'s library and test dependencies map to the Cargo targets and
native boundaries listed in the receipt. The package has no doc.go, production
build/platform variants, generated inputs, extra support files or fixtures.
The Go tests' Windows skip protects etcd fixture filenames; native tests use
in-memory/RPC fixtures. Windows runtime validation is not claimed.

No function-only seed is exposed as a completed package. The full leaf owner
and its production session/factory/worker/PD path are delivered together.
Other packages' current-master acceptance remains open.

## Behavior and design

The shared syncer retains Go's eight-item **blocking FIFO channel**, optional
PD client and single-item store contract. A missing PD client is a successful
no-op. Store failures propagate to the keeper, which logs them and continues;
there is no retry, deduplication, owner election, initial resynchronization or
periodic refresh added to Go's design.

`SysVarDef` now retains Go's GlobalConfigName metadata. Existing catalog
entries carry the empty default; the two Go entries use the existing vardef
constants for `enable_resource_metering` and `source_id`. This removes the
metadata omission without adding a second name-based dispatch list. Explicit
validated SQL assignments, including DEFAULT, publish normalized ON/OFF as
true/false and preserve numeric strings. Invalid values/scopes do not publish.
Notification is on the session write path, after hooks and before durable
publication, matching Go even when a later assignment/storage operation fails.
Startup loads, cache rebuilds and post-commit image refreshes do not notify PD.
The notifier remains attached to the Session while cluster SET swaps a scratch
global-variable image in and out.

The cluster factory shares one syncer among all sessions. Boot starts one
keeper with a request-only clone of the already-owned PD client. It creates
neither another PD worker nor another shutdown authority. Worker execution
has a background operation lifetime, independent of statement cancellation.
Shutdown wakes the receiver, releases blocked producers and joins the keeper
before the existing PD authority closes; it waits for an in-flight store.

The PD worker sends the complete generated StoreGlobalConfigRequest with an
empty default config path and the source name/value/kind/payload projection.
Per-item read errors are not sent. It uses the current leader and carries the
call's deadline through queueing/RPC execution. The pinned Go client ignores
the response-body Error and records no per-command histogram for this method;
Rust does the same. Transport failures and timeout/shutdown remain errors.
The complete PD client package, including its service-mode discovery gap P03,
is not claimed by this method integration.

Two ignored empty test placeholders were removed from tidb-session. Their
original cases now have executable owner-level coverage, rather than an
ignored stub plus a second claimed test mapping.
The old Go-only audit and testport receipt explicitly link to this replacement;
their earlier conclusions are historical, not current package acceptance.

## Evidence and validation

The SQL regression first failed after the new owner was installed but before
explicit SET was connected:

    left: []
    right: [("enable_resource_metering", "true", 0), ("source_id", "2", 0)]

Command from rust/ (offline also updated the lockfile for the new local
`tidb-domain -> tidb-proto` dependency):

    cargo test --offline -p tidb-session --lib global_config_explicit_sql_notifies_the_domain_owner

The following commands pass from rust/:

    cargo test --locked -p tidb-domain -p tidb-session -p tidb-pd-client -p tidb-server global_config
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::global_variables
    cargo test --locked -p tidb-pd-client --lib --test all
    cargo test --locked -p tidb-pd-client --test all global_config_preserves_the_pd_method_contract_without_retry
    cargo test --locked -p tidb-exec --lib order::tests
    cargo test --locked -p tidb-exec --test all configured_topn_source
    cargo test --locked -p tidb-planner --test all configured_order_limit_contract_source
    cargo check --locked -p tidb-domain -p tidb-session -p tidb-pd-client -p tidb-exec -p tidb-server --all-targets
    cargo check --locked -p tidb-domain -p tidb-session -p tidb-pd-client -p tidb-exec -p tidb-planner -p tidb-server --all-targets

The first command passes ten tests (nine focused on this repair plus an
existing config-serialization test). It covers queue saturation, blocked
producer release, nil client, store error propagation, SQL/default/clamped
values, invalid assignment/reload exclusion, failed multi-assignment order,
shared cluster-factory ownership, keeper failure continuation and in-flight
shutdown, plus real loopback PD requests/deadline/no-retry/body-error behavior.
The cluster global-variable suite passes six tests. PD passes 26 library and
46 RPC/lifecycle tests; one pre-existing real-PD etcd test remains ignored.
The final focused PD rerun additionally verifies that per-item read errors
are stripped and an explicit config path survives request construction.
The first mock-RPC attempt was denied localhost binding by the sandbox; the
network-enabled rerun passed.

The original Go package tests pass from an isolated checkout of master
`e953a09d9d5e29e60c62f42d3aacebb819af49a5`, with its pinned dependencies:

    GOTOOLCHAIN=go1.25.14 GOMAXPROCS=4 go test -p 2 -run '^Test(GlobalConfigSyncer|StoreGlobalConfig)$' -tags=intest,deadlock ./pkg/domain/globalconfigsync -count=1

The installed Go 1.27 first failed the source's map-ABI build guard; the
Go-1.25.14 rerun passed in 0.613 seconds after compilation. This package's
production, test and BUILD files contain no failpoint dependency or calls,
so failpoint rewriting was not required. The original TestMain leak check
ran as part of this invocation.

The final master refresh to `93a01d31f6` leaves all three package blobs,
Domain keeper, session publication path and go.mod/go.sum unchanged. The two
GlobalConfigName entries and constants are unchanged; the new range-count
variable is outside this leaf. The Go tests were not rerun after that refresh.
Integration received two non-overlapping planner/executor/prepared-statement
commits during the interruption. The ten focused Rust tests, six cluster
global-variable tests and all-target check were rerun on the resulting
`1e570fd1e7` publication base.
The all-target rerun first exposed four E0061 errors in existing `order.rs`
tests: incoming commit `49d65e542f` added an explicit-collation constructor
argument without migrating its test calls. A full constructor-call inventory
found twelve stale calls across executor and planner tests. Each now passes
`None`, retaining the original default-collation scenarios. The existing order,
TopN and planner contract tests and the expanded six-crate all-target gate
validate that test-only compatibility update.

Broader session checks expose three unchanged failures:

    cargo test --locked -p tidb-session --lib tests_global_vars
    cargo test --locked -p tidb-session --lib tests_global_vars -- --test-threads=1

Both have 62 passes and these three failures. An isolated **unchanged**
`021de80c8c` checkout reproduces the same three failures with 60 passes:

- `a_refused_session_max_allowed_packet_still_reports_the_truncation`: missing warning.
- `memory_limit_global_values_are_canonicalized_like_go`: rejects `90%`.
- `trace_event_global_sysvar_controls_the_flight_recorder`: classic-kernel refusal.

No existing assertion was relaxed. These failures remain outside O12's repair.
The baseline command used the same serial test invocation with
`CARGO_TARGET_DIR=/Users/qiliu/projects/tidb/rust/target` for cache reuse.

Root `make lint` and `git diff --check` pass. Publication must use the actual
hook's locked server build and then a separate fresh locked build:

    TERM=xterm git -c core.hooksPath=hooks commit -m "domain: synchronize explicit global settings with PD"
    (cd rust && cargo build --locked -p tidb-server)
    git push origin HEAD:hparser-integration

Final publication gate results are recorded in the response. The two reference
checkouts also ran `PATH="/private/tmp/tidb-globalconfig-tools:$PATH" make bazel_prepare`
with checksum-verified Bazel 7.7.1. It passed on the unchanged integration
baseline. Master built Gazelle but failed repository preparation when the Go
proxy reset downloads of github.com/ajstarks/deck, modernc.org/tcl and
modernc.org/ccorpus. The original Go package tests independently passed.
No Go/Bazel production source was changed by this repair, and reference-only
preparation artifacts were not copied. Both reference checkouts were archived
after their processes finished. Clearing the inactive Go compiler cache
recovered about 46 GiB before the remaining checks rebuilt small cache entries.

## Limits and risks

Go deliberately does not guarantee PD delivery after a failed store. This
repair preserves that behavior, including the possibility of notification
preceding a later SQL storage failure. The bounded channel applies
backpressure to global writes, as Go does. Ordinary SQL reads do not use it.
Full cluster restart/failover, real PD/TiKV deployment, Windows execution and
sysbench/TPC-C/TPC-H/YCSB benchmarks were not run. Existing parent-package
mismatches and baseline failures remain visible; no overall parity or
performance improvement is claimed.
