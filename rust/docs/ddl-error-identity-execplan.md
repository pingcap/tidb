# Preserve source DDL error identity through the worker

This living ExecPlan follows root PLANS.md and continues the shared worker
maintenance recorded in ddl-worker-continuation-execplan.md.

## Purpose and scope


The existing durable worker must preserve Go's typed error identity from its
action producer through checkpoint, cancellation, history and SQL delivery.
Numeric error codes alone cannot distinguish Schema and DDL errors. CHECK
validation must return an execution error to its worker, not an already flattened
wire response. Legacy Rust history with numeric-only errors must remain readable.

Starting integration is f8367f2db5d2ede54493c6a45c9b548f99374714. Freshly fetched
Go master is 93a01d31f6da205ae4bf376825293903a6899fdb and native client remains
6163ecfc. This repairs existing admitted paths; it introduces no action/package
port and does not accept pkg/ddl or close all of D05. The complete package and
remaining transaction/scheduler/storage-producer obligations remain in the
structural register and full-structural-parity-execplan.md.

## Progress


- [x] Fetch and compare current source; read DDL package documentation, source error producers, worker conversion/retry/cancellation and existing tests.
- [x] Add and observe failing durable identity and same-code/different-class cancellation regressions.
- [x] Preserve typed errors across all integrated persisted action producers and CHECK validation; migrate consumers and remove premature wire conversion.
- [x] Verify source identity, plain-error distinction, legacy history, retry classification, SQL compatibility and restarted-worker behavior.
- [x] Run scoped tests, all-target checks, lint and diff review; update audit evidence without closing unrelated requirements.
- [x] Commit through the actual locked-build hook and pass the fresh locked server build; repeat these gates after the receipt amendment before normal publication to hparser-integration.

## Context and plan of work


Go pkg/ddl/job_worker.go::toTError retains a terror.Error unchanged; plain errors
become DDL/CodeUnknown. rollingback.go compares source identities, and index.go
classifies retryability through terror.ToSQLError on the original typed error.
pkg/infoschema/error.go and pkg/util/dbterror/ddl_terror.go select classes at
production sites. Use the existing Rust terror/dbterror authority, not a new
numeric-to-class inference map.

First extend existing cluster_ddl_source regressions to inspect persisted RFC
identity and cancellation of a job whose prior error shares the cancellation
number but has a different class. These must fail before behavior changes.

Then add typed error transport to DdlPlanError and migrate the current persisted
CREATE/DROP/RENAME/CHECK producers using their explicit source classes. Preserve
plain decoding failures as plain errors until shared toTError conversion. Change
CheckConstraintValidator to return that same worker error and remove its private
wire-error variant/conversion. Existing numeric admission errors outside the
migrated producers retain their legacy contract, rather than inventing identities.

Finally exercise typed and plain errors through active-job serialization,
cancellation, history reload, SQL delivery and retry classification. Review every
caller changed by the validation interface and every live persisted action. Keep
disabled MV action integration and other structural gaps out of the completion
claim. No generated error table or protocol output is edited by hand.

## Validation and acceptance


From rust/, run the failing cases first and then the affected groups:

    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::schema_changes
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

From root, run make lint and git diff --check. No Go/Bazel inputs change;
bazel_prepare and Go failpoint switching are inapplicable. Server test fixtures
need the existing macOS sysctl capability. Retain logs under
/private/tmp/tidb-ddl-identity-*. A passing result must retain identity and message
through actual job/history encoding and preserve existing wire codes/SQLSTATEs.
No live distributed or benchmark claim follows from these local checks.

## Publication and recovery


Commit using TERM=xterm git -c core.hooksPath=hooks commit. The hook must pass
cd rust && cargo build --locked -p tidb-server. Repeat the locked build after
every final amendment and before git push origin HEAD:hparser-integration.
Inspect any concurrent remote change, repeat affected validation and never force
push. Verify the remote commit and clean working tree. Regression tests use
isolated stores; no deployed cluster or user data is changed.

## Surprises & Discoveries


CHECK validation currently projects to LockSqlError before the shared worker
records it. The worker then reconstructs an admission error from its number and
message. This loses both the source identity and the typed-versus-plain retry
distinction. Cancellation also compares only the number of the original error.

## Decision Log


On 2026-10-02 choose source-typed transport and producer migration over guessing
classes from numbers. Reuse the existing error authority and place wire conversion
at SQL delivery; keep old persisted rows readable with explicit legacy handling.
This follows the requested Go design and removes the premature conversion owner.

## Outcomes & Retrospective


The pre-fix cancellation run failed both selected tests: ordinary cancellation
stored an empty RFC identity, and a Schema error numbered 8214 was mistaken for
DDL cancellation and lost the rollback prefix. Log:
/private/tmp/tidb-ddl-identity-before.log. The shared worker now preserves source
errors rather than rebuilding a numeric envelope. Thirty existing numeric/string
conversion sites were migrated with explicitly selected Go classes; no new action
or package is exposed.

An existing malformed-argument test initially expected a stored code 1105. Go's
toTError instead stores DDL/CodeUnknown (-1) for the plain decoding error, and SQL
still returns 1105. The test now verifies the correct durable identity and stable
wire response. The first compile also caught an unsupported isize conversion and
an import still needed by the admission APIs; both were corrected.

Coverage includes same-number Schema/DDL errors, source/plain/numeric-only CHECK
failures, old numeric history, source formatting versus SQL message text, and
identity retained after active-job serialization, cancellation, history reload and
owner replacement. Numeric-only fresh errors are not classified as source-defined
cancellation or CHECK violation. Original package acceptance and all 75 unresolved
IDs remain open; D05 still includes other admission/storage producer losses,
complete taxonomy, rollback-transaction classification, configurable retry timing
and metrics, and missing whole-action effects.

Production files changed are cluster_ddl.rs and real_tikv_ddl.rs in tidb-exec,
and cluster_session_node/ddl.rs and sql_node.rs in tidb-server. Regression coverage
extends tidb-exec/tests/cluster_ddl_source.rs and existing transaction/worker tests.
Audit README/findings JSON/Markdown and the full structural ExecPlan record the
remaining boundary. No Go, Cargo dependency, generated artifact or native-client
file changes. Original Go pkg/ddl tests, real TiKV/etcd owner handoff, mixed-node
execution and sysbench/TPCC/TPCH/YCSB are not run here. No performance improvement
or whole-package completion is claimed. CHECK expression/storage errors whose
underlying producers still erase identity remain part of the producer/taxonomy
work; retaining the known violation identity does not accept that entire pipeline.

All scoped gates above pass: 108 planner, eight transaction, six durable-worker
and 21 SQL tests (143 total, none ignored), plus the all-target compile check.
The cancellation test also verifies old numeric history keeps an empty RFC code
and a true DDL cancellation is not wrapped again. The final shared identity gate
has no numeric-to-class fallback; two older CHECK fixtures now use the source
error that production validation returns, and a separate negative case verifies
the same bare number does not trigger rollback. Root make lint and git diff
--check pass. Logs are /private/tmp/tidb-ddl-identity-{planner,transaction,worker,
sql,check,lint}-final.log. Existing compiler/linker warnings remain.

The audit checker confirms all 85 Markdown/JSON rows agree and the statuses
remain 69 open, six partial and ten repaired. A source scan confirms the migrated
worker ranges no longer construct DdlAdmissionError::with_code and no caller
references the removed CHECK wire variant. The actual pre-commit hook passed
the locked server build, and the fresh pre-push build passed independently.
Logs are /private/tmp/tidb-ddl-identity-commit.log and
/private/tmp/tidb-ddl-identity-prepush.log. The receipt amendment must run the
hook again, followed by another fresh locked build before normal publication;
retain those logs with the -commit-final and -prepush-final suffixes. Report the
published commit after checking the remote reference and clean working tree.
