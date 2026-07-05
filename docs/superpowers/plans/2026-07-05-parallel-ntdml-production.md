# Production Parallel NTDML Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build explicit opt-in local range and DXF execution for production non-transactional `DELETE` and `UPDATE`, including signed integer handles and binary-safe single-column varchar/common handles.

**Architecture:** Keep the current serial path as the default. Add shared typed-handle planning, checkpoint, session-context, and chunk execution code under `pkg/session`, then use it from local range workers and DXF subtasks. Split coarse work by safe record-region boundaries and always advance progress with observed SQL handle values stored in durable typed checkpoints.

**Tech Stack:** Go, TiDB session/executor APIs, TiDB AST restore helpers, TiKV region cache, TiDB distributed task framework, Prometheus metrics, shell-based Docker system tests.

---

## File Structure

* Modify `pkg/session/nontransactional.go`: route explicit modes, preserve serial behavior, call shared range/DXF implementations, enforce statement shape.
* Create `pkg/session/nontransactional_handle.go`: handle descriptor, supported/rejected handle validation, encoded boundary conversion, region-boundary decode.
* Create `pkg/session/nontransactional_context.go`: worker session context capture/application for SQL mode, timezone, charset/collation, FK/check flags, user, roles, and resource group.
* Create `pkg/session/nontransactional_checkpoint.go`: checkpoint table constants, read/write/summarize/delete helpers, retry and failure status helpers.
* Create `pkg/session/nontransactional_range.go`: local range planner/executor, keyset handle scans, chunk SQL construction, retry loop, cleanup.
* Create `pkg/session/nontransactional_dxf.go`: DXF scheduler/task executor/subtask executor using the shared planner and checkpoint protocol.
* Modify `pkg/sessionctx/variable/tidb_vars.go`, `pkg/sessionctx/variable/session.go`, `pkg/sessionctx/variable/sysvar.go`, `pkg/sessionctx/variable/sysvar_test.go`: execution mode and concurrency sysvars.
* Modify `pkg/session/bootstrap.go` and `pkg/session/bootstraptest/bootstrap_upgrade_test.go`: checkpoint table bootstrap and upgrade coverage.
* Modify `pkg/disttask/framework/proto/type.go`, `pkg/disttask/framework/proto/type_test.go`, `pkg/disttask/framework/proto/step.go`, `pkg/disttask/framework/proto/step_test.go`: register NTDML task type and run step.
* Modify `pkg/metrics/session.go`, `pkg/metrics/metrics.go`, `pkg/session/metrics/metrics.go`: bounded-label metrics for tasks, chunks, retries, rows, and cleanup.
* Create or update `pkg/metrics/grafana/non_transactional_dml.json`: dashboard panels using only bounded labels.
* Create `pkg/session/nontransactional_unit_test.go`: fast tests for validators, handle encoders, checkpoint helpers, session context, metrics helpers, and retry classification.
* Modify `pkg/session/nontransactionaltest/nontransactional_test.go`: integration coverage for serial/range/DXF, int and varchar/common-handle paths, unsupported shapes, cleanup, and context propagation.
* Create `tests/ntdml/run-dxf-system-test.sh`: Docker system test for two TiDB nodes, PD, TiKV, integer and varchar clustered PK tables, skew/sparse keys, restarts, metrics, and cleanup.

## Task 1: Sysvars And Mode Routing

**Files:**
* Modify: `pkg/sessionctx/variable/tidb_vars.go`
* Modify: `pkg/sessionctx/variable/session.go`
* Modify: `pkg/sessionctx/variable/sysvar.go`
* Modify: `pkg/sessionctx/variable/sysvar_test.go`
* Modify: `pkg/session/nontransactional.go`

- [ ] **Step 1: Write sysvar tests**

Add tests named `TestNonTransactionalDMLExecutionModeSysVar` and
`TestNonTransactionalDMLConcurrencySysVar` in
`pkg/sessionctx/variable/sysvar_test.go`:

```go
func TestNonTransactionalDMLExecutionModeSysVar(t *testing.T) {
	vars := NewSessionVars(nil)
	require.Equal(t, DefTiDBNonTransactionalDMLExecutionMode, vars.NonTransactionalDMLExecutionMode)
	for _, mode := range []string{"serial", "range", "dxf"} {
		require.NoError(t, GetSysVar(TiDBNonTransactionalDMLExecutionMode).SetSession(vars, mode))
		require.Equal(t, mode, vars.NonTransactionalDMLExecutionMode)
	}
	require.ErrorContains(t, GetSysVar(TiDBNonTransactionalDMLExecutionMode).SetSession(vars, "auto"), "must be one of serial, range, dxf")
}

func TestNonTransactionalDMLConcurrencySysVar(t *testing.T) {
	vars := NewSessionVars(nil)
	require.Equal(t, DefTiDBNonTransactionalDMLConcurrency, vars.NonTransactionalDMLConcurrency)
	require.NoError(t, GetSysVar(TiDBNonTransactionalDMLConcurrency).SetSession(vars, "8"))
	require.Equal(t, 8, vars.NonTransactionalDMLConcurrency)
	require.Error(t, GetSysVar(TiDBNonTransactionalDMLConcurrency).SetSession(vars, "0"))
}
```

- [ ] **Step 2: Run the focused sysvar tests and verify they fail**

Run: `go test ./pkg/sessionctx/variable -run 'TestNonTransactionalDML(ExecutionMode|Concurrency)SysVar'`

Expected: compile failure because the new constants and fields do not exist.

- [ ] **Step 3: Add sysvar constants, defaults, session fields, and validation**

Add constants:

```go
TiDBNonTransactionalDMLExecutionMode = "tidb_nontransactional_dml_execution_mode"
TiDBNonTransactionalDMLConcurrency   = "tidb_nontransactional_dml_concurrency"
DefTiDBNonTransactionalDMLExecutionMode = "serial"
DefTiDBNonTransactionalDMLConcurrency = 4
```

Add fields:

```go
NonTransactionalDMLExecutionMode string
NonTransactionalDMLConcurrency   int
```

Register sysvars:

```go
{Scope: ScopeGlobal | ScopeSession, Name: TiDBNonTransactionalDMLExecutionMode, Value: DefTiDBNonTransactionalDMLExecutionMode, Type: TypeEnum, PossibleValues: []string{"serial", "range", "dxf"}, SetSession: func(s *SessionVars, val string) error {
	normalized := strings.ToLower(val)
	switch normalized {
	case "serial", "range", "dxf":
		s.NonTransactionalDMLExecutionMode = normalized
		return nil
	default:
		return errors.Errorf("%s must be one of serial, range, dxf", TiDBNonTransactionalDMLExecutionMode)
	}
}},
{Scope: ScopeGlobal | ScopeSession, Name: TiDBNonTransactionalDMLConcurrency, Value: strconv.Itoa(DefTiDBNonTransactionalDMLConcurrency), Type: TypeInt, MinValue: 1, MaxValue: 1024, SetSession: func(s *SessionVars, val string) error {
	s.NonTransactionalDMLConcurrency = TidbOptInt(val, DefTiDBNonTransactionalDMLConcurrency)
	return nil
}},
```

- [ ] **Step 4: Route explicit modes without changing serial**

In `HandleNonTransactionalDML`, after `checkConstraintWithShardColumn`, add:

```go
switch sessVars.NonTransactionalDMLExecutionMode {
case "range":
	return handleNonTransactionalDMLByRange(ctx, stmt, se, nodeW.GetResolveContext(), tableName, shardColumnInfo, tableSources)
case "dxf":
	return handleNonTransactionalDMLByDXF(ctx, stmt, se, nodeW.GetResolveContext(), tableName, shardColumnInfo, tableSources)
case "", "serial":
	// Continue through the existing serial implementation.
default:
	return nil, errors.Errorf("unsupported %s value %q", variable.TiDBNonTransactionalDMLExecutionMode, sessVars.NonTransactionalDMLExecutionMode)
}
```

- [ ] **Step 5: Run tests and commit**

Run: `go test ./pkg/sessionctx/variable -run 'TestNonTransactionalDML(ExecutionMode|Concurrency)SysVar'`

Run: `go test ./pkg/session -run TestNonTransactionalDML`

Commit:

```bash
git add pkg/sessionctx/variable pkg/session/nontransactional.go
git commit -m "feat: add parallel ntdml mode sysvars"
```

## Task 2: Checkpoint Table Bootstrap

**Files:**
* Modify: `pkg/session/bootstrap.go`
* Modify: `pkg/session/bootstraptest/bootstrap_upgrade_test.go`
* Create: `pkg/session/nontransactional_checkpoint.go`
* Test: `pkg/session/nontransactional_unit_test.go`

- [ ] **Step 1: Add checkpoint schema constants**

Create `pkg/session/nontransactional_checkpoint.go` with:

```go
const nonTransactionalDMLCheckpointTableName = "tidb_nontransactional_dml_checkpoint"

const createNonTransactionalDMLCheckpointTableSQL = `CREATE TABLE IF NOT EXISTS mysql.tidb_nontransactional_dml_checkpoint (
  job_id VARCHAR(128) NOT NULL,
  range_id BIGINT NOT NULL,
  mode VARCHAR(16) NOT NULL,
  dml_type VARCHAR(16) NOT NULL,
  db_name VARCHAR(64) NOT NULL,
  table_id BIGINT NOT NULL,
  physical_table_id BIGINT NOT NULL,
  handle_kind VARCHAR(32) NOT NULL,
  range_start BLOB DEFAULT NULL,
  range_start_inclusive BOOL NOT NULL DEFAULT FALSE,
  range_end BLOB DEFAULT NULL,
  range_end_inclusive BOOL NOT NULL DEFAULT FALSE,
  checkpoint BLOB DEFAULT NULL,
  status VARCHAR(16) NOT NULL,
  retry_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  error_class VARCHAR(64) DEFAULT NULL,
  error_text TEXT DEFAULT NULL,
  scanned BIGINT UNSIGNED NOT NULL DEFAULT 0,
  affected BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  finished_at TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (job_id, range_id),
  KEY idx_table_status (table_id, status),
  KEY idx_updated_at (updated_at)
)`
```

- [ ] **Step 2: Write bootstrap test**

Add a bootstrap upgrade assertion that `mysql.tidb_nontransactional_dml_checkpoint`
exists and has primary key `(job_id, range_id)`.

- [ ] **Step 3: Run bootstrap test and verify it fails**

Run: `go test ./pkg/session/bootstraptest -run TestBootstrap`

Expected: the checkpoint table is missing.

- [ ] **Step 4: Add bootstrap creation**

Call `mustExecute(s, createNonTransactionalDMLCheckpointTableSQL)` from the
latest bootstrap path and add an upgrade version function that executes the same
SQL for existing clusters.

- [ ] **Step 5: Run tests and commit**

Run: `go test ./pkg/session/bootstraptest -run 'TestBootstrap|TestUpgrade'`

Commit:

```bash
git add pkg/session/bootstrap.go pkg/session/bootstraptest/bootstrap_upgrade_test.go pkg/session/nontransactional_checkpoint.go
git commit -m "feat: bootstrap ntdml checkpoint table"
```

## Task 3: Typed Handle Descriptors

**Files:**
* Create: `pkg/session/nontransactional_handle.go`
* Test: `pkg/session/nontransactional_unit_test.go`

- [ ] **Step 1: Write validation tests**

Add table-driven tests for `_tidb_rowid`, signed int clustered PK,
`varchar(128) collate utf8mb4_bin primary key clustered`, `varbinary(128)
primary key clustered`, unsigned PK rejection, non-binary collation rejection,
composite common handle rejection, partition rejection, and secondary-index
shard-column rejection.

- [ ] **Step 2: Define handle kinds and descriptors**

```go
type nonTransactionalDMLHandleKind string

const (
	nonTransactionalDMLHandleInt       nonTransactionalDMLHandleKind = "int"
	nonTransactionalDMLHandleExtra     nonTransactionalDMLHandleKind = "tidb_rowid"
	nonTransactionalDMLHandleCommonBin nonTransactionalDMLHandleKind = "common_binary"
)

type nonTransactionalDMLHandleDescriptor struct {
	kind       nonTransactionalDMLHandleKind
	tableInfo  *model.TableInfo
	columnInfo *model.ColumnInfo
	columnName ast.ColumnName
	fieldType  types.FieldType
}
```

- [ ] **Step 3: Implement supported-handle validation**

Implement:

```go
func buildNonTransactionalDMLHandleDescriptor(se sessiontypes.Session, stmt *ast.NonTransactionalDMLStmt, tableName *ast.TableName, shardColumnInfo *model.ColumnInfo, tableSources []*ast.TableSource) (*nonTransactionalDMLHandleDescriptor, error)
```

The function returns actionable errors for every rejected shape in the spec and
only returns `common_binary` for single-column clustered `char`, `varchar`,
`binary`, or `varbinary` primary keys with binary ordering.

- [ ] **Step 4: Run tests and commit**

Run: `go test ./pkg/session -run 'TestNonTransactionalDML.*Handle'`

Commit:

```bash
git add pkg/session/nontransactional_handle.go pkg/session/nontransactional_unit_test.go
git commit -m "feat: validate typed ntdml handles"
```

## Task 4: Typed Boundaries And SQL Conditions

**Files:**
* Modify: `pkg/session/nontransactional_handle.go`
* Create or modify: `pkg/session/nontransactional_range.go`
* Test: `pkg/session/nontransactional_unit_test.go`

- [ ] **Step 1: Write encode/decode and SQL construction tests**

Test integer, `_tidb_rowid`, `varchar binary`, and `varbinary` values. For SQL
conditions assert strict checkpoint lower bounds and inclusive upper bounds:

```sql
(`id` > 'v1:pacer_largepayload0001') AND (`id` <= 'v1:pacer_largepayload0100')
```

- [ ] **Step 2: Add boundary type**

```go
type nonTransactionalDMLBoundary struct {
	value     types.Datum
	encoded   []byte
	inclusive bool
	hasValue  bool
}
```

- [ ] **Step 3: Implement datum/handle conversion**

Implement:

```go
func encodeNonTransactionalDMLBoundary(sc *stmtctx.StatementContext, desc *nonTransactionalDMLHandleDescriptor, datum types.Datum, inclusive bool) (nonTransactionalDMLBoundary, error)
func decodeNonTransactionalDMLBoundary(sc *stmtctx.StatementContext, desc *nonTransactionalDMLHandleDescriptor, encoded []byte, inclusive bool) (nonTransactionalDMLBoundary, error)
func astValueExprForNonTransactionalDMLBoundary(desc *nonTransactionalDMLHandleDescriptor, boundary nonTransactionalDMLBoundary) *driver.ValueExpr
```

- [ ] **Step 4: Implement AST range conditions**

Implement:

```go
func buildNonTransactionalDMLRangeCondition(desc *nonTransactionalDMLHandleDescriptor, lower *nonTransactionalDMLBoundary, upper *nonTransactionalDMLBoundary) ast.ExprNode
```

Use `>` for checkpoint lower bounds, `>=` for inclusive coarse lower bounds,
`<` for exclusive coarse upper bounds, and `<=` for observed chunk upper bounds.

- [ ] **Step 5: Run tests and commit**

Run: `go test ./pkg/session -run 'TestNonTransactionalDML.*Boundary|TestNonTransactionalDML.*Condition'`

Commit:

```bash
git add pkg/session/nontransactional_handle.go pkg/session/nontransactional_range.go pkg/session/nontransactional_unit_test.go
git commit -m "feat: add typed ntdml range boundaries"
```

## Task 5: Shared Worker Session Context

**Files:**
* Create: `pkg/session/nontransactional_context.go`
* Test: `pkg/session/nontransactional_unit_test.go`

- [ ] **Step 1: Write context capture/application tests**

Assert current DB, SQL mode, timezone, charset/collation, FK/check flags, user,
active roles, resource group, statement resource group, and redact-log setting
are copied into a worker session.

- [ ] **Step 2: Implement captured context**

```go
type nonTransactionalDMLSessionContext struct {
	CurrentDB         string
	SysVars           map[string]string
	User              *auth.UserIdentity
	ActiveRoles       []*auth.RoleIdentity
	ResourceGroup     string
	StmtResourceGroup string
}
```

Implement `captureNonTransactionalDMLSessionContext` and
`applyNonTransactionalDMLSessionContext` using `SessionVars.SetSystemVar`,
`AuthWithoutVerification` when a privilege manager exists, `ActiveRoles`, and
`SetResourceGroupName`.

- [ ] **Step 3: Run tests and commit**

Run: `go test ./pkg/session -run 'TestNonTransactionalDML.*SessionContext'`

Commit:

```bash
git add pkg/session/nontransactional_context.go pkg/session/nontransactional_unit_test.go
git commit -m "feat: propagate ntdml worker session context"
```

## Task 6: Checkpoint CRUD And Retry Classification

**Files:**
* Modify: `pkg/session/nontransactional_checkpoint.go`
* Test: `pkg/session/nontransactional_unit_test.go`

- [ ] **Step 1: Write checkpoint helper tests**

Use an in-memory store to verify done checkpoint write/read, failed checkpoint
retention, successful job cleanup, failed job retention, retryable error
classification, and ambiguous checkpoint verification behavior.

- [ ] **Step 2: Implement row model and helpers**

```go
type nonTransactionalDMLCheckpointStatus string

const (
	nonTransactionalDMLCheckpointDone   nonTransactionalDMLCheckpointStatus = "done"
	nonTransactionalDMLCheckpointFailed nonTransactionalDMLCheckpointStatus = "failed"
)

type nonTransactionalDMLCheckpoint struct {
	JobID      string
	RangeID    int64
	Mode       string
	DMLType    string
	DBName     string
	TableID    int64
	HandleKind nonTransactionalDMLHandleKind
	Lower      *nonTransactionalDMLBoundary
	Upper      *nonTransactionalDMLBoundary
	Checkpoint *nonTransactionalDMLBoundary
	Status     nonTransactionalDMLCheckpointStatus
	ErrorClass string
	ErrorText  string
	Scanned    uint64
	Affected   uint64
}
```

Implement `writeNonTransactionalDMLCheckpoint`, `loadNonTransactionalDMLCheckpoint`,
`summarizeNonTransactionalDMLCheckpoints`, `deleteNonTransactionalDMLCheckpoints`,
and `isNonTransactionalDMLRetryableError`.

- [ ] **Step 3: Run tests and commit**

Run: `go test ./pkg/session -run 'TestNonTransactionalDML.*Checkpoint|TestNonTransactionalDML.*Retry'`

Commit:

```bash
git add pkg/session/nontransactional_checkpoint.go pkg/session/nontransactional_unit_test.go
git commit -m "feat: persist typed ntdml checkpoints"
```

## Task 7: Local Range Planner And Executor

**Files:**
* Modify: `pkg/session/nontransactional.go`
* Modify: `pkg/session/nontransactional_range.go`
* Test: `pkg/session/nontransactionaltest/nontransactional_test.go`

- [ ] **Step 1: Write local range integration tests**

Cover signed integer `DELETE`, signed integer `UPDATE`, `_tidb_rowid`, binary
varchar clustered PK delete/update, production-shaped prefixes, idempotent JSON
nullout update, non-idempotent update documentation result, concurrency > 1,
cleanup after success, and retention after a failed chunk.

- [ ] **Step 2: Implement range context**

```go
type nonTransactionalDMLRangeContext struct {
	Stmt              *ast.NonTransactionalDMLStmt
	Descriptor        *nonTransactionalDMLHandleDescriptor
	SessionCtx        nonTransactionalDMLSessionContext
	JobID             string
	Mode              string
	DMLType           string
	DBName            string
	CurrentDB         string
	FromSQL           string
	HandleExprSQL     string
	OriginalWhereSQL  string
	OriginalCondition ast.ExprNode
	BatchSize         int
}
```

- [ ] **Step 3: Implement keyset scan and chunk mutation**

Use SQL of this shape:

```sql
SELECT <handle> FROM <from> WHERE (<original>) AND <range predicates> ORDER BY <handle> LIMIT <batch>
```

Then restore DML AST with:

```sql
(<range lower from checkpoint or range start>) AND (<handle> <= <last observed handle>) AND (<original>)
```

- [ ] **Step 4: Implement local goroutine execution**

Workers read work items from a channel, create worker sessions, apply captured
context, execute chunks with retry, write checkpoints in the mutation
transaction, cancel peers on first non-ignored failure, summarize, and delete
successful checkpoints before returning success.

- [ ] **Step 5: Run tests and commit**

Run: `go test ./pkg/session/nontransactionaltest -run 'TestNonTransactionalDML.*Range|TestNonTransactionalDML.*Serial'`

Commit:

```bash
git add pkg/session/nontransactional.go pkg/session/nontransactional_range.go pkg/session/nontransactionaltest/nontransactional_test.go
git commit -m "feat: execute local parallel ntdml ranges"
```

## Task 8: Region-Boundary Range Planning

**Files:**
* Modify: `pkg/session/nontransactional_handle.go`
* Modify: `pkg/session/nontransactional_range.go`
* Test: `pkg/session/nontransactional_unit_test.go`

- [ ] **Step 1: Write region-boundary tests**

Build record keys with `tablecodec.EncodeRowKeyWithHandle` for int and
`kv.NewCommonHandle` encoded varchar datums. Verify decodeable boundaries become
typed boundaries, wrong-table keys are ignored, undecodable keys are coalesced,
and generated ranges have no gaps or overlaps.

- [ ] **Step 2: Implement region loading**

Use:

```go
tikvStore, ok := se.GetStore().(helper.Storage)
regions, err := tikvStore.GetRegionCache().LoadRegionsInKeyRange(tikv.NewBackofferWithVars(ctx, 20000, nil), startKey, endKey)
```

where `startKey, endKey := tablecodec.GetTableHandleKeyRange(tableID)`.

- [ ] **Step 3: Convert region boundaries**

Decode region start and end keys with `tablecodec.DecodeRecordKey`, require the
target table id, require the descriptor kind, decode common-handle datums with
`kv.CommonHandle.Data`, and drop unsafe boundaries.

- [ ] **Step 4: Run tests and commit**

Run: `go test ./pkg/session -run 'TestNonTransactionalDML.*Region|TestNonTransactionalDML.*RangePlan'`

Commit:

```bash
git add pkg/session/nontransactional_handle.go pkg/session/nontransactional_range.go pkg/session/nontransactional_unit_test.go
git commit -m "feat: split ntdml work by record regions"
```

## Task 9: DXF Task Type And Execution

**Files:**
* Modify: `pkg/disttask/framework/proto/type.go`
* Modify: `pkg/disttask/framework/proto/type_test.go`
* Modify: `pkg/disttask/framework/proto/step.go`
* Modify: `pkg/disttask/framework/proto/step_test.go`
* Create: `pkg/session/nontransactional_dxf.go`
* Test: `pkg/session/nontransactionaltest/nontransactional_test.go`

- [ ] **Step 1: Write proto tests**

Assert `proto.NonTransactionalDML` maps to a stable integer and
`proto.NonTransactionalDMLStepRun` renders as `run`.

- [ ] **Step 2: Register task type**

Add:

```go
NonTransactionalDML TaskType = "NonTransactionalDML"
```

and:

```go
case NonTransactionalDML:
	return 4
```

Add step:

```go
const NonTransactionalDMLStepRun Step = 1
```

- [ ] **Step 3: Implement DXF metadata**

Store executable SQL, display SQL, handle descriptor metadata, session context,
batch size, and planned ranges. Store encoded handles as bytes in JSON fields,
not raw literals in labels or log keys.

- [ ] **Step 4: Implement scheduler and subtask executor**

Register scheduler, task executor, and cleanup. Scheduler creates subtasks from
shared region-planned ranges. Executor applies captured session context, resumes
from checkpoint, loops keyset chunks, updates realtime summary, and returns
retryable errors to DXF only when classification says retryable.

- [ ] **Step 5: Make explicit DXF fail when unavailable**

When `tidb_enable_dist_task` is off or `storage.GetTaskManager()` fails, return
an error containing `DXF mode requires distributed task framework` and do not
fall back to local range.

- [ ] **Step 6: Run tests and commit**

Run: `go test ./pkg/disttask/framework/proto ./pkg/session/nontransactionaltest -run 'TestTaskType|TestStep2Str|TestNonTransactionalDML.*DXF'`

Commit:

```bash
git add pkg/disttask/framework/proto pkg/session/nontransactional_dxf.go pkg/session/nontransactionaltest/nontransactional_test.go
git commit -m "feat: run ntdml through dxf"
```

## Task 10: Observability, Metrics, And Dashboard

**Files:**
* Modify: `pkg/metrics/session.go`
* Modify: `pkg/metrics/metrics.go`
* Modify: `pkg/session/metrics/metrics.go`
* Create: `pkg/metrics/grafana/non_transactional_dml.json`
* Test: `pkg/session/nontransactional_unit_test.go`

- [ ] **Step 1: Write metrics helper tests**

Assert labels are limited to mode, DML type, result/state, and status. Assert no
SQL, predicates, user names, or key values are accepted by helper methods.

- [ ] **Step 2: Add metrics**

Add counters/histograms for task lifecycle, chunk lifecycle, rows scanned,
affected rows, retry count, duration, and cleanup result using bounded labels.

- [ ] **Step 3: Add dashboard JSON**

Include panels for running tasks, succeeded/failed/canceled tasks, duration,
chunk throughput, rows affected, retries, failures, and cleanup results.

- [ ] **Step 4: Run validation and commit**

Run: `go test ./pkg/session -run 'TestNonTransactionalDML.*Metric'`

Run: `python3 -m json.tool pkg/metrics/grafana/non_transactional_dml.json >/dev/null`

Commit:

```bash
git add pkg/metrics pkg/session/metrics pkg/session/nontransactional_unit_test.go
git commit -m "feat: add ntdml observability"
```

## Task 11: Documentation And Operator Semantics

**Files:**
* Modify: `docs/design/2022-03-25-non-transactional-dml.md`
* Create: `docs/design/2026-07-05-production-parallel-non-transactional-dml.md`

- [ ] **Step 1: Document syntax and sysvars**

State default serial behavior, explicit `range` and `dxf`, concurrency, batch
size through `LIMIT`, and unsupported explicit mode errors.

- [ ] **Step 2: Document supported and rejected matrix**

List every statement/key/table shape from the feature contract with exact user
action guidance.

- [ ] **Step 3: Document failure and cancel semantics**

State chunk at-least-once behavior, predicate recheck, no global atomicity,
idempotent recommended cases, non-idempotent update risk, checkpoint cleanup,
failed diagnostic retention, `KILL QUERY`, and DXF cancel path.

- [ ] **Step 4: Commit docs**

Commit:

```bash
git add docs/design/2022-03-25-non-transactional-dml.md docs/design/2026-07-05-production-parallel-non-transactional-dml.md
git commit -m "docs: describe production parallel ntdml"
```

## Task 12: Docker System Test

**Files:**
* Create: `tests/ntdml/run-dxf-system-test.sh`

- [ ] **Step 1: Write system test script**

Script responsibilities:

```bash
BUILD_TIDB=${BUILD_TIDB:-0}
NTDML_ROWS=${NTDML_ROWS:-20000}
NTDML_PAYLOAD_BYTES=${NTDML_PAYLOAD_BYTES:-1024}
NTDML_CONCURRENCY=${NTDML_CONCURRENCY:-4}
```

The script builds TiDB when `BUILD_TIDB=1`, starts PD, TiKV, and two TiDB nodes,
loads signed-int and `varchar(180) collate utf8mb4_bin primary key clustered`
tables, runs DXF delete/update workloads, restarts submitter/owner/executor
nodes during jobs, verifies counts and metrics, and cleans all containers and
temporary directories on exit.

- [ ] **Step 2: Run shell lint and a small local system test**

Run: `bash -n tests/ntdml/run-dxf-system-test.sh`

Run: `BUILD_TIDB=1 NTDML_ROWS=2000 NTDML_PAYLOAD_BYTES=256 tests/ntdml/run-dxf-system-test.sh`

- [ ] **Step 3: Commit**

Commit:

```bash
git add tests/ntdml/run-dxf-system-test.sh
git commit -m "test: add dxf ntdml system coverage"
```

## Task 13: Full Verification

**Files:**
* All changed files.

- [ ] **Step 1: Run affected unit and integration packages**

Run:

```bash
go test ./pkg/session ./pkg/session/nontransactionaltest ./pkg/sessionctx/variable ./pkg/disttask/framework/proto ./pkg/disttask/framework/storage ./pkg/metrics
```

- [ ] **Step 2: Run dashboard and diff checks**

Run:

```bash
python3 -m json.tool pkg/metrics/grafana/non_transactional_dml.json >/dev/null
git diff --check
```

- [ ] **Step 3: Run Docker system test**

Run:

```bash
BUILD_TIDB=1 tests/ntdml/run-dxf-system-test.sh
```

- [ ] **Step 4: Record blockers exactly**

If a full command cannot complete locally because of machine, Docker, or time
limits, record the exact command, failure output, environment limit, and CI
command needed in `docs/design/2026-07-05-production-parallel-non-transactional-dml.md`.

- [ ] **Step 5: Final review commit**

Run `git status --short`, inspect the diff, and commit any verification-note
edits with:

```bash
git add docs/design/2026-07-05-production-parallel-non-transactional-dml.md
git commit -m "docs: record ntdml verification status"
```
