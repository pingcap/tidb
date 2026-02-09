# DDL Development Checklist (where to change, testing, pitfalls)

This doc is a practical checklist for making DDL changes without breaking online DDL guarantees.

## 1) Decide the correct layer (do not “fix it in SQL executor”)

Use this decision table:

| Change type | Correct place |
|---|---|
| Pure statement parsing/AST changes | `pkg/parser/*` |
| Planner builds DDL plan nodes | `pkg/planner/*` |
| Transaction boundary / generic AST dispatch | `pkg/executor/ddl.go` |
| Convert statement → job args / job submission / waiting | `pkg/ddl/executor.go` |
| Persistent step execution, schema state transitions, meta writes | `pkg/ddl/job_worker.go` + the per-action handlers (e.g. `pkg/ddl/table.go`, `pkg/ddl/schema.go`, `pkg/ddl/index.go`, `pkg/ddl/modify_column.go`, `pkg/ddl/partition.go`) |
| Job args encoding/decoding, job version compat | `pkg/meta/model/*` |
| Schema sync / versioning mechanisms | `pkg/ddl/schemaver/*`, `pkg/ddl/schema_version.go` |

If your logic must survive restart/owner transfer, it belongs in the **job execution path** (DDL workers), not `pkg/executor/`.

## 2) Adding a new DDL action (high-level steps)

1. Parser/AST: introduce syntax and AST nodes if needed.
2. Planner: map AST to a DDL plan type (or reuse existing patterns).
3. SQL executor dispatch: add/extend `pkg/executor/ddl.go` switch only if a new AST type is introduced.
4. DDL executor (statement → job):
   - Add a method in `pkg/ddl/executor.go` (or reuse an existing one).
   - Build `model.Job` + typed args (prefer v2 if supported by the job type).
5. Worker execution:
   - Implement job step transitions and meta writes.
   - Ensure schema diff/version update and `WaitVersionSynced` happen at the right boundaries.
6. Regression tests:
   - Add a unit test under `pkg/ddl/*_test.go` (prefer targeted tests).
   - If the behavior is user-visible in SQL results, consider integration tests under `tests/integrationtest/`.

## 3) Testing commands (recommended)

Unit test (targeted):

```bash
pushd pkg/ddl
go test -run TestXxx --tags=intest
popd
```

Integration tests (when behavior is user-visible / cross-module):

```bash
pushd tests/integrationtest
./run-tests.sh -t <TestName>
popd
```

If you need to update the recorded result set, use `-r`:

```bash
pushd tests/integrationtest
./run-tests.sh -r <TestName>
popd
```

Failpoints:

- If the package uses `failpoint.` or `testfailpoint.`, enable failpoints before running tests and disable afterwards:

```bash
make failpoint-enable && (
  pushd pkg/ddl
  go test -run TestXxx --tags=intest
  rc=$?
  popd
  make failpoint-disable
  exit $rc
)
```

Build system note (Bazel):

- If you add/remove/move Go files (including new `_test.go`), run `make bazel_prepare` and include generated `*.bazel/*.bzl` changes.

## 4) Debugging and observability tips

SQL-level:

- `ADMIN SHOW DDL JOBS`
- `ADMIN SHOW DDL JOB QUERIES`
- `ADMIN CANCEL DDL JOBS <job_id>`

Code-level:

- Start from `pkg/ddl/executor.go:DoDDLJobWrapper` (wait loop) and follow job ID through submit/schedule/worker.
- Worker logs use DDL loggers (e.g. `logutil.DDLLogger()` in `pkg/ddl/*`).

## 5) Common pitfalls (quick scan before you send a PR)

- **Forgot schema sync**: meta updated but no global schema version update / no `WaitVersionSynced`.
- **Non-idempotent step**: job step can be re-run and corrupts state (owner transfer / retry).
- **Progress not persisted**: reorg restarts from zero after retry.
- **Args compatibility**: job args change breaks decode for existing jobs (esp. v1/v2 boundary).
- **Wrong boundary**: implementing “schema change” in `pkg/executor/` instead of `pkg/ddl/`.
