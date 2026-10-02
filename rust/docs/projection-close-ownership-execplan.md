# Join parallel projection work before closing its child

This living ExecPlan follows repository `PLANS.md`. It repairs the existing
executor's E06 lifetime finding, not transcreation or acceptance of the complete
Go `pkg/executor` package.

## Purpose and context


A parent stopping after LIMIT, or after a projection error, must be able to
close the executor without leaving evaluation running against statement state.
Go `pkg/executor/projection.go::Close` signals finish, waits for its fetcher and
workers, drains resources, and only then closes children. An evaluation already
running may finish. Rust `rust/crates/tidb-executor/src/projection.rs` currently
drops its receiver and immediately closes its child; Arc memory ownership does
not establish operation completion.

Both repositories were freshly fetched and pulled without changes: integration
6ea3668555ed095243a5d2f2952919dfdd419f65 and native client master
19a56ccda1e128218cd33c69709038219aced9bc. Normative Go master is
93a01d31f6da205ae4bf376825293903a6899fdb; local projection.go matches it. There
is no pkg/executor/doc.go. Native-client changes are unnecessary for this repair.

## Progress


- [x] Fetch current branches; verify the source Close contract and E06.
- [x] Add regressions and observe five failures before production edits.
- [x] Implement one completion/cancellation owner for projection tasks.
- [x] Validate projection and affected pool callers; run lint and review diff.
- [x] Update audit evidence and verify the actual hook's locked server build;
  final publication uses the mandatory fresh-build/push chain below.

## Milestones and plan of work


First add tests beside the existing projection fixtures. Force two evaluations
to overlap, return the first result (or error/panic), keep the other blocked,
then close. The child must observe that evaluation finished before it closes.
The old implementation must fail this assertion. Exercise reopen and drop too.

Next add a crate-private TaskGroup in worker_pool.rs. This is a lifetime owner
for this executor's tasks on the existing shared CPU queue, not a new pool or
thread set. It marks cancellation, removes its queued tasks, releases their
captures outside the queue lock, and waits for running tasks. Completion must
also survive unwinding. Other queue users retain their current scheduling.
Projection stores this owner before shared resources, submits through it, and
retires it before child close/reopen or dropping the executor. Remove the old
fire-and-forget path. Do not interrupt an evaluator midway through a chunk.

Finally run targeted tests, exercise the affected executor library, and compile
consumers. Update the E06 register and this receipt without granting acceptance
to unported package owners. Commit and push to the authorized integration branch.

## Validation and acceptance


From repository rust/ run these commands (record results below):

    cargo test --locked -p tidb-executor --lib projection::tests::projection_lifetime
    cargo test --locked -p tidb-executor --lib projection::tests::
    cargo test --locked -p tidb-executor --lib worker_pool::tests::
    cargo test --locked -p tidb-executor --lib
    cargo check --locked -p tidb-executor --all-targets

The first command must fail against unchanged production and pass after the
repair. Tests must prove completion before child retirement, including errors,
panics, reopen, and queued task cancellation without affecting another owner.
No Go or Bazel input changes are planned, so bazel_prepare is not required.
From repository root run make lint and git diff --check. Commit with:

    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: join projection workers before child close'

The actual pre-commit hook must build the locked Rust server. After the final
commit run a fresh build immediately before push:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Verify git ls-remote origin refs/heads/hparser-integration matches HEAD and the
working tree is clean. No live-cluster or workload-performance claim is made.

## Surprises & Discoveries


The module documentation still describes parallel projection as deferred even
though the worker pipeline is live. Go's existing parallel required-rows test
is skipped for goroutine scheduling; the Rust close regression needs explicit
gates rather than assumptions about which worker starts first.

## Decision Log


Use a per-execution task group on the existing CPU pool. Merely waiting for
result messages cannot reliably own error/unwind cleanup, and dedicating
blocking lanes would add a new thread-scheduling policy. Queue cancellation
must not drop arbitrary captured resources while holding the shared queue lock.

## Outcomes & Retrospective


E06 is repaired in the candidate. Five initial lifecycle regressions fail before
production edits and pass after the fix. Six final lifecycle tests include child
fetch errors; three task-group tests cover queued cancellation, other owners,
claimed-but-not-started work, running completion and unwind resource release.
Other unresolved findings are outside this receipt. The actual commit hook
passes the locked server build. The final receipt amendment also uses that
hook, followed by the fresh locked-build/push chain below.

## Idempotence, recovery, and interfaces


Tests and checks are repeatable. Do not reset unrelated work or force push.
If a test blocks, release its test gate and join its threads before reporting
failure. If a gate fails, fix the candidate and rerun the affected checks.
TaskGroup is crate-private with new/submit and joined Drop; no external API,
dependency, generated input or SQL result format changes are required.

## Validation evidence


The final focused commands from rust/ pass:

    cargo test --locked -p tidb-executor --lib projection::tests::
    cargo test --locked -p tidb-executor --lib worker_pool::tests::
    cargo test --locked -p tidb-executor --test all physical_expand_source
    cargo test --locked -p tidb-executor --test all insert_select_source
    cargo check --locked -p tidb-executor --all-targets

They pass 15 projection, ten pool and two integration cases (27 total). The
projection suite compares serial/parallel rows and required-row propagation,
and exercises statement parameter/warning state. The integration controls cover
projection through Expand and INSERT SELECT. The final library-wide command
`cargo test --locked -p tidb-executor --lib` passes 1,468 cases and fails 36.
The unchanged integration commit passes 1,459 and fails exactly the same 36
test IDs. A temporary copy of only the two edited source files was saved;
HEAD versions were tested in place and an EXIT trap restored both candidates.
No other files or changes were reset. No new failing IDs appear in the final
comparison. The baseline failures include existing sequence/default parser,
statistics, error metadata, auto-ID, join, sort and TopN cases; this receipt
does not claim a green complete executor suite.

Logs are /private/tmp/tidb-projection-close-{red,green,projection,pool,expand,
insert-select,lib,baseline,check,lint}.log. The red run was a runtime failure
in all five cases, not a compile failure. An initial fixture compile error
(ExecError lacks Display) was corrected before that run. Existing compiler
warnings remain. Root `make lint` passed after retrying outside the network
sandbox so its pinned Go linter could be installed. Root `git diff --check`
and the following formatting check pass:

    rustfmt --edition 2021 --check rust/crates/tidb-executor/src/projection.rs rust/crates/tidb-executor/src/worker_pool.rs

The Go projection file equals freshly fetched master (`git diff origin/master
-- pkg/executor/projection.go` is empty). Go tests were not rerun: the source
Close order is the contract, while the regression executes the existing Rust
owner. No Go/Bazel input changed and no failpoint instrumentation was needed
for these Rust tests. No live TiKV, sanitizer, cross-platform, or
sysbench/TPC-C/TPC-H/YCSB measurement is claimed.

## Design review and remaining risk


TaskGroup uses an independent completion channel whose EOF means all task
captures were released, including unwind paths. Cancelled queued closures are
dropped outside the shared queue lock. Work popped by a CPU worker retains a
completion token and checks cancellation before evaluating. Already-running
evaluations finish before Close returns, as in Go; Close can therefore wait
for a slow expression. A normally drained group avoids scanning/locking the
global queue. Early cancellation scans the ready queue once, preserves other
owners' order and creates no new worker threads. Throughput impact is unmeasured.
Rust's explicit Drop and reopen retirement preserve this lifetime even when a
caller omits Close; no new SQL semantics or expression behavior is introduced.

The register now has 85 tracked, 72 unresolved (66 open/six partial), 13 repaired.
Historical reviews retain their earlier counts and pins. The native client is
unchanged and already synchronized at 19a56cc; no artificial dependency change
is needed for this executor repair.

## Publication gates


The actual `TERM=xterm git -c core.hooksPath=hooks commit` succeeded and ran
`cd rust && cargo build --locked -p tidb-server`. Its log is
/private/tmp/tidb-projection-close-commit.log. The receipt completion amendment
uses the same hook, with output in /private/tmp/tidb-projection-close-amend.log.
After the final commit, run this chain from repository root; no failed gate
may be bypassed and no force push is permitted:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration && git rev-parse HEAD && git ls-remote origin refs/heads/hparser-integration && git status --short

The final build/push log is /private/tmp/tidb-projection-close-prepush.log.
HEAD and the remote branch must match and the working tree must be clean.
This update records completed validation and makes the remaining publication
operation enforce its own required build on the final commit.
