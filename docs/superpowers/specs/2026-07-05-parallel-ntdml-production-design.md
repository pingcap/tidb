# Production Parallel Non-Transactional DML Design

## Context

TiDB already supports non-transactional DML through `BATCH ON ... LIMIT ...`.
The existing execution path keeps serial behavior as the default and builds all
shard jobs before running them. That is safe for the current feature contract,
but it is not enough for multi-hour cleanup jobs over very large tables because
planning can become a full materialized boundary scan and execution is tied to a
single TiDB SQL node.

The prototype PR at `39f8270a8ac190cc7849e0cafbbd4d46e2042ea8` is useful
research material, but it is not the production contract. In particular, a
signed-integer-only splitter misses the motivating production shape: clustered
single-column `varchar` primary keys stored as common handles.

TTL migration remains out of scope.

## Recommended Approach

Use a typed-handle parallel executor with two explicit opt-in modes:

* `serial`: existing behavior and the default.
* `range`: local TiDB workers execute bounded chunks from deterministic handle
  ranges.
* `dxf`: TiDB distributed task framework owns the job and executes the same
  bounded chunk protocol across TiDB nodes.

The splitter must not depend on generated midpoint values for correctness.
Instead, planning uses TiKV record-region boundaries as coarse scheduling
ranges when they can be decoded safely for the target table and handle type.
Workers then use SQL keyset scans to find observed handle values and mutate only
up to the last observed handle in the chunk. Correctness comes from observed
handle checkpoints, explicit range predicates, and rechecking the original DML
predicate during mutation.

Explicit `range` and `dxf` modes must fail early with actionable errors when
the table, statement, handle type, collation, or cluster capability is not
supported. They must not silently fall back to serial execution. Explicit `dxf`
mode must also fail if distributed task execution is unavailable, rather than
quietly switching to local range mode.

## Feature Contract

Supported statements:

* Single-table `DELETE`.
* Single-table `UPDATE`.
* Existing `BATCH ON <handle> LIMIT <batch-size>` syntax.
* The `LIMIT` value is the maximum rows per chunk transaction.
* Parallelism is configured by session/global variables or equivalent syntax:
  execution mode and concurrency.

Required semantics:

* Default behavior remains serial.
* Parallel modes are explicit opt-in.
* Every chunk transaction is bounded by both `LIMIT` and handle predicates.
* The original DML predicate is re-evaluated when the mutation runs.
* No global atomicity is claimed across chunks.
* `DELETE` and idempotent `UPDATE` statements are first-class supported use
  cases.
* Non-idempotent `UPDATE` statements are allowed only with documented
  at-least-once chunk semantics. A failed or ambiguous chunk can be retried
  after checkpoint verification; users must not infer exactly-once behavior for
  arbitrary expressions such as `c = c + 1`.
* Raw SQL and predicates must not appear in metric labels. Logs must use TiDB's
  redaction/display SQL mechanisms.

## Support Matrix

Supported handles:

* `_tidb_rowid`.
* Single signed integer clustered primary key.
* Single-column clustered common handle with `char` or `varchar`, only when the
  effective collation has binary byte ordering.
* Single-column clustered common handle with `binary` or `varbinary`.

Rejected shapes:

* Partitioned tables.
* Composite common handles.
* Unsigned integer clustered primary keys.
* Non-binary string collations for string common handles.
* Prefix indexes or non-full-length primary key columns.
* Secondary-index shard columns.
* Generated midpoint splitting for arbitrary strings.
* Multi-table `DELETE` or `UPDATE`.
* `INSERT INTO ... SELECT`.
* Statements that update the handle column used for chunking.
* User variables, system variable references, or session-local functions in
  parallel worker predicates unless separately proven safe and captured.

## Common-Handle Correctness

String/common-handle support is limited to a single primary-key column so SQL
ordering, TiKV encoded record key order, and checkpoint comparison can be
mapped directly. Binary collations are required for string columns because
locale-aware collations can order values differently than raw key bytes.

Checkpoint values are always observed values returned by a worker scan, not
synthetic string midpoints. Region boundary keys are only scheduling hints. A
region boundary can become a SQL range boundary only when it decodes as a record
key for the target table and supported handle shape. Undecodable or unsafe
boundaries are coalesced into adjacent ranges.

Stats, histograms, row counts, and sampled values may be used for estimates or
progress display, but never as correctness-critical state.

## Range Planning

Planning captures the table metadata, handle descriptor, original predicate,
DML kind, batch size, concurrency, and session context. It then loads record
key ranges from PD/region cache, converts safe decoded region boundaries into
typed SQL handle boundaries, removes empty or overlapping ranges, and creates
deterministic work items.

Each worker repeatedly:

1. Reads the latest durable checkpoint for its work item.
2. Scans handles with the original predicate, range lower bound, range upper
   bound, strict checkpoint lower bound, `ORDER BY handle`, and `LIMIT`.
3. Runs the mutation with the same lower/upper bounds and `handle <=
   last_observed_handle`.
4. Rechecks the original predicate in the mutation statement.
5. Writes a checkpoint in the same transaction as the mutation result.

Parallel range mode uses local goroutines limited by the configured
concurrency. DXF mode stores the same work-item metadata in distributed task
subtasks, so submitter, owner, and executor restarts can continue from durable
state.

## Checkpoint Lifecycle

The checkpoint table must be shared by range and DXF modes and must store:

* job id and work item id.
* mode, DML kind, database, table id, physical table id, and handle kind.
* lower and upper range boundaries, including inclusivity.
* latest committed checkpoint handle, encoded in a type-preserving form.
* status, retry count, error class, error message, affected rows, scanned rows,
  created time, updated time, and finished time.

Successful chunks write progress transactionally with the mutation. Retryable
pre-commit errors do not create terminal checkpoint rows. Ambiguous commit
errors must be resolved by reading the checkpoint before deciding whether to
retry. Permanent statement errors mark the work item failed and retain
diagnostics.

Successful jobs delete their checkpoint rows after the final summary is
available. Failed or canceled jobs retain bounded diagnostic rows. Cleanup
failure after successful mutation must be returned to the user and surfaced in
metrics because leaking millions of successful rows is not production ready.

## Failure Semantics

Retryable transaction errors are retried by the owning worker or by a later DXF
executor after reading checkpoint state. Schema changes, unsupported metadata
changes, context cancellation, and permanent SQL errors are classified
separately. A schema change that invalidates the handle descriptor fails the job
instead of continuing with stale assumptions.

If the submitter TiDB exits during local range mode, local work stops with the
session. DXF mode is the production mode for restart survival: task metadata and
checkpoints must let the job survive submitter, owner, executor, and rolling
TiDB restarts without losing committed progress.

## Session, Security, and Resource Context

Planning and worker sessions must apply the submitter context needed for
consistent SQL evaluation:

* current database.
* SQL mode.
* timezone.
* charset and collation.
* foreign-key and check-constraint flags.
* user identity and active roles.
* resource group.
* redaction setting and display SQL behavior.

The worker must execute with the same privilege-sensitive identity and roles
that were validated at submit time, or reject execution if that cannot be
represented safely.

## Observability and Operations

Operators need progress and control during long jobs. The feature must expose:

* mode, job id, table id, state, created/updated/finished times.
* ranges total, running, completed, failed, and canceled.
* chunks total, rows scanned, rows affected, retries, and cleanup state.
* bounded-cardinality metrics for lifecycle, duration, rows, chunks, retries,
  failures, and cleanup.
* dashboard JSON validation when a dashboard is changed.
* cancellation through `KILL QUERY` for the submitter wait path and through a
  documented distributed-task cancel path for DXF jobs.

No metric label may contain raw SQL, predicates, user-provided literals, or
range values.

## Test Plan

Unit tests must cover sysvar validation, statement validation, supported and
rejected handle metadata, typed boundary encode/decode, integer and
common-handle SQL construction, no-gap/no-overlap range generation, retry and
ambiguous-commit classification, checkpoint cleanup and retention, session
context capture/application, bounded metric labels, and dashboard JSON validity.

Integration tests must cover unchanged serial behavior, local range and DXF
`DELETE`/`UPDATE` on signed integer handles, `_tidb_rowid`, and single-column
binary `varchar` common handles, production-shaped key prefixes such as
`v1:pacer_largepayload...`, idempotent JSON nullout updates, documented
non-idempotent update behavior, actual concurrency greater than one, independent
repeated jobs, resource group/user/role propagation, progress visibility,
successful checkpoint cleanup, failed checkpoint retention, and clear errors
without fallback for unsupported explicit modes.

Docker system tests must use real PD, TiKV, and at least two TiDB nodes built
from the branch. They must run DXF delete and update workloads over signed
integer and varchar/common-handle clustered primary key tables, include sparse
or skewed keys, restart submitter/owner/executor nodes while jobs run, verify
final row and update counts, inspect progress/checkpoint cleanup/retention,
query metrics endpoints, and clean up containers, ports, temporary directories,
and TiDB processes.

## Production-Readiness Checklist

* Serial remains default and unchanged.
* Parallel execution is explicit opt-in.
* Supported key and statement matrix is enforced by tests and docs.
* Varchar/common-handle clustered primary keys work for binary-safe ordering.
* Unsupported explicit modes fail with actionable messages.
* Chunk mutations are bounded and recheck original predicates.
* Checkpoints are durable, typed, and cleaned after success.
* Failed jobs retain useful diagnostics.
* DXF survives submitter, owner, executor, and rolling TiDB restarts.
* Resource group, identity, roles, and session context propagate consistently.
* Operators can observe progress, throughput, retries, failures, cleanup, and
  duration.
* Operators can cancel or stop long jobs through documented controls.
* Metrics have bounded labels and logs are redaction-aware.
* Unit, integration, and Docker system tests prove both integer and
  varchar/common-handle paths.
