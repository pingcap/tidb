# Generate persisted CHECK errors from Go's shared prototypes

This living ExecPlan follows root PLANS.md and continues the error transport
repair published as 4eb761a825b7bc1b0a9b4db89c12dfaddb706a19.

## Purpose and scope


Persisted CHECK jobs must retain both the source error identity and its message
construction rules. Removing numeric envelopes did not remove private message
templates. Go's error prototypes already own formatting, argument precision and
redaction. Route existing CHECK producers through that owner and preserve Go's
choice of error, original identifier casing and no-error states. This maintains
existing handlers; it does not accept the entire pkg/ddl or external errors
package, add an action or close D05.

Go master remains 93a01d31f6da205ae4bf376825293903a6899fdb. Source references are
pkg/ddl/constraint.go, table.go, pkg/util/dbterror/ddl_terror.go and pinned
github.com/pingcap/errors normalize.go. The existing shared Rust formatter is
tidb-error/src/mysql/error.rs; TerrorError and dbterror already retain the source
catalog. No generated output needs editing.

## Progress


- [x] Review Go error generation and all three existing CHECK action handlers.
- [x] Observe pre-fix failures for CHECK state selection, mixed-case identifiers and validator delivery; reproduce the independent raw-argument checkpoint mismatch.
- [x] Route nine CHECK error-generation sites through shared prototype argument generation; remove the invented ALTER error branch and schema-dependent argument-encoding switch.
- [x] Run scoped errors, persisted DDL, worker and SQL regressions, compile checks and lint.
- [x] Update audit evidence and pass the actual locked-build hook and fresh pre-push build; repeat both after the receipt amendment before normal publication.

## Plan of work


First extend rust/crates/tidb-exec/tests/cluster_ddl_source.rs using its existing
job-table and metadata fixtures. Unsupported ADD states must persist ddl:8210
with Go's SchemaState rendering. Unsupported DROP states select ddl:8204 and
Go's formatter output, including extra arguments. ALTER's enforcing switch has
no default error: an unmatched state must only checkpoint the job, without a
schema mutation or added error count. Missing-constraint messages preserve the
submitted name's original case. Add a production validator projection regression
in the existing server DDL test module.

Successful CHECK steps must re-encode decoded arguments even when the action
does not change a schema version. Failed steps retain original raw arguments
through the existing shared worker policy. Verify both using an unknown field
in a persisted job's original argument JSON.

Add TerrorError::generate_with_stack_by_args in the existing shared error
authority, reusing its catalog formatter and native backtrace. Use that method
in cluster_ddl.rs CHECK producers and cluster_session_node/ddl.rs validation;
do not build a separate CHECK error catalog. Keep Go's deliberately custom
GetTableInfoAndCancelFaultJob message as a custom message. Existing schema and
other action producer/taxonomy limitations remain tracked separately.

Then validate the error package, affected persisted action suite, shared worker,
transaction and SQL tests, plus all-target compilation. This is a maintenance
receipt, not a replacement for complete source-package inventory and acceptance.

## Validation and acceptance


From rust/:

    cargo test --locked -p tidb-exec --test all persisted_check_source_
    cargo test --locked -p tidb-server --lib check_validation_source_message
    cargo test --locked -p tidb-error
    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::schema_changes
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

From root run make lint and git diff --check. No Go/Bazel input changes;
bazel_prepare and Go failpoint switching do not apply. Keep exact logs under
/private/tmp/tidb-ddl-generation-*. Confirm errors through active-row/history
serialization and SQL conversion, not just by checking a helper's return value.
Real TiKV/etcd, mixed-node operation and workload benchmarks remain unverified.

## Publication and recovery


Commit with TERM=xterm git -c core.hooksPath=hooks commit. The actual hook must
pass cd rust && cargo build --locked -p tidb-server. Run that build afresh after
the final commit/amend and before a normal push to hparser-integration. Verify
the remote SHA and a clean tree; never force-push. Tests use isolated fixtures
and do not change deployed schema or data.

## Surprises & Discoveries


Go's DROP invalid-state branch selects ErrInvalidDDLJob, not ErrInvalidDDLState.
Its extra formatting arguments are observable and must not be replaced with a
more attractive invented diagnostic. ALTER has no default error in that switch.
The full-transaction rollback marker's Go producers are placement/affinity action
effects absent from the Rust persisted handlers; adding an unused marker would
not resolve that remaining integration gap.

## Decision Log


On 2026-10-02 reuse the shared catalog formatter and explicit source prototypes.
Patching individual message strings would leave the duplicated owner in place.
Preserve unusual source behavior when it is unambiguous; no new refusal or
success state is invented for malformed persisted metadata.

## Outcomes & Retrospective


Published predecessor 4eb761a825 retains source identity; this follow-up removes
nine CHECK-specific message construction sites. ADD now renders SchemaState
through the shared catalog. DROP selects the source ddl:8204 prototype, and
ALTER no longer invents an error/schema mutation for an unmatched enforcing
state. Missing-constraint names retain original case; validation uses the lower
name that Go's verification SQL returns. Generated errors carry native stacks.
The shared argument formatter also retains an unformatted prototype for zero
arguments, matching Go GetMsg, and checks Unicode precision without mutating
the prototype.

Three pre-fix regressions failed with mismatched messages; logs are
/private/tmp/tidb-ddl-generation-before.log and
/private/tmp/tidb-ddl-generation-validator-before.log. A later independent
regression failed because a successful ALTER step retained an unknown raw-arg
field; /private/tmp/tidb-ddl-generation-args-before.log. Removing CHECK's
schema-dependent encoding flag fixes it while preserving raw fields after ADD
and DROP errors. Initial test setup needed an explicit GoField constructor and
explicit stored constraint metadata; those setup failures are not bug evidence.

All final gates pass: 41 error-package tests, 110 persisted planner tests, eight
transaction tests, seven worker tests and 21 SQL tests: 187 total, none ignored.
The all-target compile check, root make lint, git diff --check and 85-row audit
consistency check pass. Final logs are /private/tmp/tidb-ddl-generation-{errors,
planner,transaction,worker,sql,check}-final.log and
/private/tmp/tidb-ddl-generation-lint.log. Existing compiler/linker warnings
remain. The actual commit-hook build and an independent fresh pre-push build
passed; logs are /private/tmp/tidb-ddl-generation-commit.log and
/private/tmp/tidb-ddl-generation-prepush.log. The receipt amendment must repeat
the hook and then run a fresh locked build, retained with the -commit-final and
-prepush-final log suffixes, before normal publication. Verify the remote SHA
and clean tree before reporting publication.

The Go message oracle also passes with the cached supported Go 1.25.14
toolchain. Default Go 1.27 cannot compile this checkout's hack/checkMapABI, and
a partial Go 1.26 cache was unusable; neither attempt is claimed as validation.
The oracle runs the integration checkout's source definitions, whose relevant
prototypes and SchemaState rendering were compared with origin/master. It is
not a full master pkg/ddl test run. Reproduce from the repository root:

    cat > /private/tmp/tidb-ddl-generation-oracle.go <<'GO'
    package main
    import (
        "fmt"
        "github.com/pingcap/tidb/pkg/meta/model"
        "github.com/pingcap/tidb/pkg/parser/ast"
        "github.com/pingcap/tidb/pkg/util/dbterror"
    )
    func main() {
        fmt.Println(dbterror.ErrInvalidDDLState.GenWithStackByArgs("constraint", model.StateDeleteOnly))
        fmt.Println(dbterror.ErrInvalidDDLJob.GenWithStackByArgs("constraint", model.StatePublic))
        fmt.Println(dbterror.ErrConstraintNotFound.GenWithStackByArgs(ast.NewCIStr("MissingCheck")))
        fmt.Println(dbterror.ErrCheckConstraintIsViolated.GenWithStackByArgs(ast.NewCIStr("MixedCheck").L))
    }
    GO
    GOTOOLCHAIN=go1.25.14 go run /private/tmp/tidb-ddl-generation-oracle.go

Observed output, retained in /private/tmp/tidb-ddl-generation-go.log:

    [ddl:8210]Invalid constraint state: delete only
    [ddl:8204]Invalid DDL job%!(EXTRA string=constraint, model.SchemaState=public)
    [ddl:3940]Constraint 'MissingCheck' does not exist.
    [ddl:3819]Check constraint 'mixedcheck' is violated.

Production files changed are tidb-error/src/terror.rs,
tidb-exec/src/cluster_ddl.rs and tidb-server/src/cluster_session_node/ddl.rs under
rust/crates. Tests extend tidb-error/tests/terror_source.rs,
tidb-exec/tests/cluster_ddl_source.rs and the existing server DDL module. This
ExecPlan, full-structural-parity-execplan.md and the audit README/findings
JSON/Markdown record the scope. No Go, generated output, Cargo dependency or
client-rust file changes. Existing typed/legacy history remains readable.

D05 remains partial and the register retains 75 unresolved IDs: remaining
producer/taxonomy gaps, complete action effects, transaction reset and retry
configuration/metrics are still open. Other CHECK expression/storage producers
can still erase their original error identity. No complete upstream package,
live distributed interoperability or performance result is claimed. Diagnostics
and malformed-state behavior intentionally change to the reviewed source;
real TiKV/etcd and sysbench/TPCC/TPCH/YCSB were not run.
