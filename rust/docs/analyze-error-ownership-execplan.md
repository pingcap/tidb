# Preserve ANALYZE execution errors through SQL delivery

## Purpose / Big Picture


ANALYZE must return the original expression/column diagnostic, including its
MySQL code and SQLSTATE, when sample evaluation fails. Go master returns the
error from FillVirtualColumnValue through its worker result, and converts it to
text only for job history. Rust currently labels these failures Unsupported,
then stringifies them again at session/cluster boundaries. This repairs the
existing ANALYZE error owner and callers; it does not accept the complete Go
executor package or close the lower datatype portion of K03.

## Progress


- [x] Pull implementation branches and Go master; inspect shared and tier-specific error paths.
- [x] Demonstrate local and cop-backed server regressions before production edits.
- [x] Preserve typed execution errors through local/cluster ANALYZE and remote virtual-row projection.
- [x] Verify focused regressions, affected suites with five controlled baseline failures, lint and all-target checking.
- [x] Pass the actual commit hook and prepare the fresh locked-build publication gate.

## Context and Orientation


The baseline is TiDB 62cc7ab3e39786ea6a6fe1591f090ac85e5e1077;
Go master is 93a01d31f6da205ae4bf376825293903a6899fdb. Native client-rust is
19a56ccda1e128218cd33c69709038219aced9bc and unchanged. Root AGENTS.md and
PLANS.md apply, with no deeper Rust instructions and no executor doc.go.
No Go/Bazel/generated input changes are planned, so bazel_prepare is not needed.

The common computation error is tidb-executor/src/analyze.rs::AnalyzeError.
Local table scans in analyze/kv.rs erase KvTableError, and session/analyze_arm.rs
then erases AnalyzeError. Cluster virtual_samples.rs erases DriverError before
finish_samples returns cluster_analyze::AnalyzeError; real_tikv_analyze.rs erases
that into ClusterAnalyzeError::Other. sql_node.rs owns final cluster delivery.
Existing DriverError/ExecError already preserve expression, cast and storage SQL
errors and provide the canonical MySQL rendering. Reuse that ownership.

Go pkg/executor/analyze_col_sampling.go::decodeSampleDataWithVirtualColumn returns virtual-fill
errors directly; analyze workers retain statistics.AnalyzeResults.Err and
finishJobWithLog records the message without replacing the returned error.

## Milestones and Plan of Work


First extend existing generated-column SQL and ANALYZE sample suites. Add a
virtual expression to already stored data so ANALYZE encounters a real expression
failure. Verify the same error reaches local and cop-backed cluster sessions,
that subsequent statements work, and failed analysis publishes no statistics.
Capture pre-fix failures before changing production files.

Then add an execution-error carrier to the shared computation error using the
existing DriverError, and preserve it through local and cluster transport. All
callers of the common owner must migrate together. Keep unsupported shapes and
other existing generic errors generic; do not infer codes from message strings.
Keep commit ambiguity, killed errors, partition-warning handling and job-history
text behavior intact. No new statistics algorithm or runtime feature is needed.

Finally run affected ANALYZE tests and all-target cargo checking for executor,
exec, session and server; run root make lint, review formatting without unrelated
churn, and record residuals in the existing K03 audit. No live cluster or workload
performance claim follows from fixtures.

## Validation and Publication


From rust/, run focused tests before and after the repair, then the existing
ANALYZE suites. Exact commands and observed results will be added below. From the
root, run GOTOOLCHAIN=go1.25.14 make lint and git diff --check. Commit using
TERM=xterm git -c core.hooksPath=hooks commit so the actual hook runs the locked
server build. After the final commit or amendment, publish only through:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify the remote SHA and clean checkout. If the branch moves, integrate without
force and repeat affected gates. Preserve all unrelated edits and concurrent
commits. These are existing-owner repairs, not whole-package transcreation.

## Surprises & Discoveries


Both unchanged-production SQL regressions fail: ANALYZE returns 1105/HY000
instead of the original COT(0) execution error 1690/22003. ALTER adds the virtual
column after the base row exists, so the error occurs during sample evaluation.

## Decision Log


Decision: carry existing execution errors through the entire ANALYZE computation
and result path, instead of adding a code special case at the final server.
Rationale: rendering destroys error identity before the wire boundary and is
repeated independently in local and cluster execution. Date/author: 2026-10-02,
Codex.

Decision: also migrate kv_table/table_scan.rs remote row projection to the shared
ExecError conversion. The end-to-end diagnostic comparison exposed another
string adapter at that caller; it is the same generated evaluation owner as the
ANALYZE sample path. Prove it red before repair. Date/author: 2026-10-02, Codex.

## Outcomes & Retrospective


Generated execution errors now remain in the shared DriverError/ExecError owner
through local scans, sample computation, cluster results and SQL delivery. Remote
row projection uses the same conversion instead of Debug text. The shared error
conversion matches every AnalyzeError variant explicitly so future carriers need
an explicit delivery decision. Generic unsupported/encoding/quota errors retain
their existing messages and code. Commit ambiguity and killed/partition errors
retain their existing independent handling.

K03 remains partial. Lower datatype error/value identities, generated-expression
diagnostic context and legacy ENUM/SET collation context remain unresolved. For
example, generated COT currently emits the shorter DOUBLE overflow message while
ordinary expression rewriting includes its argument. This repair preserves the
original generated error rather than inventing its missing context. Other
ANALYZE storage/default/build producers still use generic diagnostics; the
entire executor package is not accepted. No workload speedup is claimed.


## Validation Evidence and Files Changed


The initial new SQL regressions both failed against unchanged production:
ANALYZE returned 1105/HY000 for a generated COT(0) execution error whose code is
1690/22003. The strengthened cluster regression separately failed at its remote
virtual-read assertion before that string adapter was removed. The final cases
assert identical generated-read/ANALYZE messages, correct code/state, failed job
history, absent histograms after failure, usable sessions and a successful ANALYZE
after removing the invalid generated column. A shared-owner unit test also
preserves column/storage SQL identity and evaluation provenance, while checking
that ordinary unsupported, encoding and quota errors keep their existing form.

Affected suites report 172 passes, five failures and six ignored tests. Fresh
unchanged-production controls reproduce all five failures: executor generated
histogram count and partition pseudo status, two session EXPLAIN estimates, and
server partition-scoped analysis refusing partition p2. No new failure remains.
The controls also reproduce the final local ANALYZE and remote-reader regressions;
all nine saved production files were restored byte-for-byte. The two SQL
regressions and one shared-owner test pass after repair. These results do not
claim that broad suites are green or that their baseline expectations are all
correct. Their test IDs and log hashes are retained in
[the machine-readable receipt](parity/current-audit/analyze-error-validation.json).

Commands from rust/ (scoped suites were selected for the modified shared owner,
its table decoder, SQL adapters and existing cancellation/commit/quota coverage):

    cargo test --locked -p tidb-executor --lib analyze -- --test-threads=1
    cargo test --locked -p tidb-executor --test all row_decoder_source -- --test-threads=1
    cargo test --locked -p tidb-exec --lib analyze -- --test-threads=1
    cargo test --locked -p tidb-session --test all analyze -- --test-threads=1
    cargo test --locked -p tidb-session --lib analyze -- --test-threads=1
    cargo test --locked -p tidb-server --lib analyze -- --test-threads=1
    cargo test --locked -p tidb-server --lib generated_read_policy -- --test-threads=1
    cargo test --locked -p tidb-executor --lib analyze::tests::execution_errors_retain_column_and_storage_identity -- --test-threads=1
    cargo test --locked -p tidb-session --test all analyze_preserves_generated_execution_error -- --test-threads=1
    cargo test --locked -p tidb-server --lib analyze_preserves_generated_execution_error -- --test-threads=1
    cargo check --locked -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets

Root gates are GOTOOLCHAIN=go1.25.14 make lint and git diff --check. Changed Rust
files were formatted with rustfmt --edition 2021 --config skip_children=true
--emit stdout while retaining unchanged baseline formatting hunks. Final checks
repeat the server suite and all-target check after the last admission caller
migration, plus the shared-owner/local regression tests after baseline restoration.

Production edits are tidb-executor/src/analyze.rs, analyze/kv.rs and
kv_table/table_scan.rs; tidb-exec/src/cluster_analyze.rs,
cluster_analyze/virtual_samples.rs and real_tikv_analyze.rs;
tidb-session/src/analyze_arm.rs; and tidb-server/src/sql_node.rs and
cluster_session_node/mod.rs. Tests extend the existing analyze unit module,
session generated_column_write_source.rs and server unistore_cop.rs. This receipt,
the main plan, audit registers and validation JSON retain scope and evidence.

No original Go package tests, live TiKV/TiFlash run, full workspace test run or
sysbench/TPC-C/TPC-H/YCSB benchmark was performed. The success-path sampling,
transaction and scheduling algorithms are unchanged. The intended compatibility
change is preservation of existing execution errors instead of generic 1105;
lower producer/message gaps remain explicit above. Existing compiler, linker and
jemalloc configuration warnings remain.


Final all-target compilation and root lint pass. The final server ANALYZE suite
has 32 passes and the same partition-name failure as baseline; the shared owner
and local regression pass. The branch was fetched again with no concurrent
update before these publication gates. Diff review retains only the intended
shared-error migrations, regressions and audit evidence.

## Publication Gates


The actual `TERM=xterm git -c core.hooksPath=hooks commit` passed its mandatory
`cd rust && cargo build --locked -p tidb-server`. This receipt-only amendment
uses the same hook. After the final amendment, publication reruns that build and
pushes only on success:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify HEAD against `git ls-remote origin refs/heads/hparser-integration` and
verify `git status --short`. Native client-rust and the dependency remain current
at 19a56cc and are unchanged by this repair.
